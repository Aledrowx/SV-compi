from __future__ import annotations

import gc
import io
import logging
import shutil
import subprocess
import os
import re
import threading
import time
import uuid
import tempfile
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Callable, Iterable

from flask import Flask, jsonify, request
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload, MediaFileUpload
from pypdf import PdfReader, PdfWriter
import fitz  # PyMuPDF: ruta especial para PDFs gigantes, sin alterar el flujo normal

# ================================================================
# CONFIGURACIÓN GENERAL (OPTIMIZADA PARA RAILWAY / HASTA 8 GB RAM)
# ================================================================

PORT = int(os.getenv("PORT", "8080"))
MAX_CONCURRENT_JOBS = max(1, int(os.getenv("MAX_CONCURRENT_JOBS", "2")))
UPLOAD_CHUNK_SIZE = max(256 * 1024, int(os.getenv("UPLOAD_CHUNK_SIZE", str(4 * 1024 * 1024))))
JOB_TTL_SECONDS = max(60, int(os.getenv("JOB_TTL_SECONDS", str(6 * 60 * 60))))
GC_COLLECT_EVERY_FILES = max(5, int(os.getenv("GC_COLLECT_EVERY_FILES", "20")))
SISTEMA_MAESTRO_KEY = os.getenv("SISTEMA_MAESTRO_KEY", "").strip()

# PDFs gigantes: solo activa una ruta alternativa cuando una fuente supera
# este tamaño. Los PDFs normales siguen usando el compilador actual (pypdf).
HEAVY_PDF_THRESHOLD_MB = max(100, int(os.getenv("HEAVY_PDF_THRESHOLD_MB", "150")))
HEAVY_PDF_THRESHOLD_BYTES = HEAVY_PDF_THRESHOLD_MB * 1024 * 1024

logging.basicConfig(level=logging.INFO)
app = Flask(__name__)
app.secret_key = os.getenv("FLASK_SECRET_KEY", "").strip() or os.urandom(32)
app.config.update(
    SESSION_COOKIE_HTTPONLY=True,
    SESSION_COOKIE_SAMESITE="Lax",
    PERMANENT_SESSION_LIFETIME=60 * 60 * 12,  # 12 horas
)

job_semaphore = threading.BoundedSemaphore(MAX_CONCURRENT_JOBS)
job_executor = ThreadPoolExecutor(max_workers=MAX_CONCURRENT_JOBS, thread_name_prefix="pdf-job")
jobs_lock = threading.RLock()
jobs: dict[str, dict[str, Any]] = {}
request_index: dict[str, str] = {}

output_locks_guard = threading.RLock()
output_locks: dict[str, threading.RLock] = {}


# ================================================================
# AUTENTICACIÓN Y GOOGLE DRIVE
# ================================================================

def get_bearer_token() -> str:
    if SISTEMA_MAESTRO_KEY:
        received_key = (request.headers.get("X-Sistema-Maestro-Key") or "").strip()
        if received_key != SISTEMA_MAESTRO_KEY:
            raise ValueError("Clave privada del Sistema Maestro inválida.")

    auth_header = (request.headers.get("Authorization") or "").strip()
    if not auth_header.lower().startswith("bearer "):
        raise ValueError("Falta el encabezado Authorization: Bearer <token>.")

    token = auth_header.split(" ", 1)[1].strip()
    if not token:
        raise ValueError("El token OAuth está vacío.")
    return token


def get_drive_service(token: str):
    credentials = Credentials(token=token)
    return build("drive", "v3", credentials=credentials, cache_discovery=False)


def download_drive_file_to_path(service, file_id: str, directory: str) -> str:
    if not file_id:
        raise ValueError("Se recibió un ID de archivo vacío.")

    path = os.path.join(directory, f"source_{uuid.uuid4().hex}.pdf")
    media_request = service.files().get_media(fileId=file_id, supportsAllDrives=True)
    with open(path, "wb") as output:
        downloader = MediaIoBaseDownload(output, media_request, chunksize=UPLOAD_CHUNK_SIZE)
        done = False
        while not done:
            _, done = downloader.next_chunk(num_retries=5)
    return path


def open_pdf_reader_path(path: str, file_id: str) -> tuple[Any, PdfReader]:
    handle = open(path, "rb")
    try:
        reader = PdfReader(handle, strict=False)
        if reader.is_encrypted:
            decrypted = reader.decrypt("")
            if not decrypted:
                raise ValueError(f"El PDF {file_id} está protegido con contraseña.")
        return handle, reader
    except Exception:
        handle.close()
        raise


def write_pdf_to_path(writer: PdfWriter, directory: str, prefix: str) -> str:
    path = os.path.join(directory, f"{prefix}_{uuid.uuid4().hex}.pdf")
    with open(path, "wb") as output:
        writer.write(output)
    return path


def upload_pdf_path(service, path: str, folder_id: str, filename: str, replace_existing: bool = False) -> dict[str, Any]:
    if not folder_id:
        raise ValueError("No se indicó la carpeta de destino.")
    safe_name = sanitize_filename(filename)
    media = MediaFileUpload(path, mimetype="application/pdf", resumable=True, chunksize=UPLOAD_CHUNK_SIZE)
    metadata = {"name": safe_name, "parents": [folder_id]}
    output_lock = get_output_lock(folder_id, safe_name)

    with output_lock:
        uploaded = service.files().create(
            body=metadata, media_body=media,
            fields="id,name,webViewLink,size,createdTime",
            supportsAllDrives=True
        ).execute(num_retries=5)

        removed_duplicates = 0
        if replace_existing:
            removed_duplicates = trash_duplicate_files(service, folder_id, safe_name, str(uploaded.get("id") or ""))

    return {
        "id": uploaded.get("id", ""),
        "url": uploaded.get("webViewLink", ""),
        "final_name": uploaded.get("name", safe_name),
        "size": uploaded.get("size", ""),
        "created_time": uploaded.get("createdTime", ""),
        "duplicates_removed": removed_duplicates,
    }


def trash_file_ids(service, file_ids: Iterable[str]) -> None:
    for file_id in file_ids:
        if not file_id:
            continue
        try:
            service.files().update(fileId=file_id, body={"trashed": True}, fields="id,trashed", supportsAllDrives=True).execute(num_retries=5)
        except Exception:
            app.logger.exception("No se pudo retirar la salida parcial %s", file_id)


def escape_drive_query_literal(value: str) -> str:
    return str(value or "").replace("\\", "\\\\").replace("'", "\\'")


def get_output_lock(folder_id: str, filename: str) -> threading.RLock:
    key = f"{folder_id}::{filename}".casefold()
    with output_locks_guard:
        lock = output_locks.get(key)
        if lock is None:
            lock = threading.RLock()
            output_locks[key] = lock
        return lock


def trash_duplicate_files(service, folder_id: str, filename: str, keep_file_id: str) -> int:
    escaped_folder = escape_drive_query_literal(folder_id)
    escaped_name = escape_drive_query_literal(filename)
    query = f"'{escaped_folder}' in parents and name = '{escaped_name}' and trashed = false"

    removed = 0
    page_token: str | None = None

    while True:
        response = service.files().list(
            q=query, fields="nextPageToken,files(id,name)",
            pageToken=page_token, spaces="drive",
            supportsAllDrives=True, includeItemsFromAllDrives=True
        ).execute(num_retries=5)

        for item in response.get("files", []):
            file_id = str(item.get("id") or "").strip()
            if not file_id or file_id == keep_file_id:
                continue
            service.files().update(fileId=file_id, body={"trashed": True}, fields="id,trashed", supportsAllDrives=True).execute(num_retries=5)
            removed += 1

        page_token = response.get("nextPageToken")
        if not page_token:
            break
    return removed


def sanitize_filename(filename: str) -> str:
    cleaned = re.sub(r'[\\/:*?"<>|]+', "-", str(filename or "")).strip()
    cleaned = re.sub(r"\s+", " ", cleaned)
    if not cleaned:
        cleaned = "SALIDA.pdf"
    if not cleaned.lower().endswith(".pdf"):
        cleaned += ".pdf"
    return cleaned


def split_pdf_extension(filename: str) -> tuple[str, str]:
    safe = sanitize_filename(filename)
    return safe[:-4], ".pdf"


def validate_payload(data: Any, required: Iterable[str]) -> dict[str, Any]:
    if not isinstance(data, dict):
        raise ValueError("El cuerpo debe ser un JSON válido.")
    for key in required:
        if data.get(key) in (None, "", []):
            raise ValueError(f"Falta el campo obligatorio: {key}.")
    return data


def as_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    return str(value or "").strip().lower() in {"1", "true", "si", "sí", "yes"}


# ================================================================
# ALMACÉN DE TRABAJOS ASÍNCRONOS
# ================================================================

def cleanup_expired_jobs() -> None:
    cutoff = time.time() - JOB_TTL_SECONDS
    with jobs_lock:
        expired_ids = [job_id for job_id, job in jobs.items() if float(job.get("updated_at", 0)) < cutoff]
        for job_id in expired_ids:
            request_id = str(jobs[job_id].get("request_id") or "").strip()
            jobs.pop(job_id, None)
            if request_id and request_index.get(request_id) == job_id:
                request_index.pop(request_id, None)


def update_job(job_id: str, **changes: Any) -> None:
    with jobs_lock:
        job = jobs.get(job_id)
        if not job:
            return
        job.update(changes)
        job["updated_at"] = time.time()


def update_job_progress(job_id: str, **progress_changes: Any) -> None:
    with jobs_lock:
        job = jobs.get(job_id)
        if not job:
            return
        progress = dict(job.get("progress") or {})
        progress.update(progress_changes)
        job["progress"] = progress
        job["updated_at"] = time.time()


def create_or_reuse_job(*, job_type: str, request_id: str, token: str, payload: dict[str, Any]) -> tuple[str, bool]:
    cleanup_expired_jobs()
    normalized_request_id = str(request_id or "").strip()

    with jobs_lock:
        if normalized_request_id:
            existing_job_id = request_index.get(normalized_request_id)
            if existing_job_id and existing_job_id in jobs:
                return existing_job_id, False

        job_id = uuid.uuid4().hex
        jobs[job_id] = {
            "job_id": job_id,
            "request_id": normalized_request_id,
            "job_type": job_type,
            "job_state": "queued",
            "status": "accepted",
            "created_at": time.time(),
            "updated_at": time.time(),
            "progress": {
                "processed_files": 0,
                "total_files": len(payload.get("file_ids") or []),
                "parts_created": 0,
                "pages_in_current_part": 0,
            },
        }
        if normalized_request_id:
            request_index[normalized_request_id] = job_id

    job_executor.submit(run_background_job, job_id, job_type, token, payload)
    return job_id, True


def get_public_job(job_id: str) -> dict[str, Any] | None:
    cleanup_expired_jobs()
    with jobs_lock:
        job = jobs.get(job_id)
        if not job:
            return None
        public_job = {
            "job_id": job.get("job_id"),
            "job_type": job.get("job_type"),
            "job_state": job.get("job_state"),
            "status": job.get("status"),
            "progress": dict(job.get("progress") or {}),
            "created_at": job.get("created_at"),
            "updated_at": job.get("updated_at"),
        }
        result = job.get("result")
        if isinstance(result, dict):
            public_job.update(result)
        if job.get("detail"):
            public_job["detail"] = job.get("detail")
        return public_job


# ================================================================
# COMPILADOR Y TOMOS
# ================================================================

ProgressCallback = Callable[..., None]


def get_drive_pdf_size(service, file_id: str) -> tuple[int, str]:
    """Devuelve (tamaño_en_bytes, nombre) sin descargar el PDF."""
    meta = service.files().get(
        fileId=str(file_id),
        fields="id,name,size,mimeType",
        supportsAllDrives=True,
    ).execute(num_retries=5)
    try:
        size = int(meta.get("size") or 0)
    except (TypeError, ValueError):
        size = 0
    return size, str(meta.get("name") or file_id)


def compile_in_parts_heavy(
    service, file_ids: list[str], destination_folder_id: str,
    output_filename: str, page_limit: int,
    progress_callback: ProgressCallback | None = None,
    replace_existing: bool = False, strict_mode: bool = True,
    expected_source_count: int = 0,
) -> tuple[list[dict[str, Any]], list[dict[str, str]]]:
    """
    Ruta exclusiva para PDFs gigantes.

    Descarga cada fuente a /tmp con la función existente y usa PyMuPDF
    para copiar páginas desde archivos físicos. No reemplaza el flujo normal:
    compile_in_parts() solo entra aquí si detecta una fuente >= umbral pesado.
    """
    if page_limit < 1:
        raise ValueError("El límite de páginas debe ser mayor que cero.")
    if expected_source_count and len(file_ids) != expected_source_count:
        raise ValueError("La cantidad de fuentes recibidas no coincide con la esperada.")

    base_name, extension = split_pdf_extension(output_filename)
    parts: list[dict[str, Any]] = []
    errors: list[dict[str, str]] = []
    part_number = 1
    pages_in_current_part = 0
    output_doc = fitz.open()

    def report(**changes: Any) -> None:
        if progress_callback:
            progress_callback(**changes)

    def flush_current(temp_dir: str, is_split: bool) -> None:
        nonlocal output_doc, pages_in_current_part, part_number
        if pages_in_current_part == 0:
            return

        if is_split or part_number > 1:
            part_name = f"{base_name} (Parte {part_number}) ({pages_in_current_part} páginas){extension}"
        else:
            part_name = f"{base_name} ({pages_in_current_part} páginas){extension}"

        output_path = os.path.join(temp_dir, f"compilado_pesado_{uuid.uuid4().hex}.pdf")
        try:
            # garbage=0 y deflate=False evitan recomprimir planos/imágenes gigantes.
            # Esto prioriza bajo uso de RAM y velocidad; conserva los streams.
            output_doc.save(output_path, garbage=0, deflate=False, clean=False)
            output_doc.close()
            output_doc = fitz.open()

            uploaded = upload_pdf_path(
                service,
                output_path,
                destination_folder_id,
                part_name,
                replace_existing=replace_existing,
            )
            uploaded["paginas"] = pages_in_current_part
            parts.append(uploaded)
            report(
                parts_created=len(parts),
                pages_in_current_part=0,
                last_created_file=uploaded.get("final_name", part_name),
                processing_mode="heavy_disk",
            )
        finally:
            if os.path.exists(output_path):
                try:
                    os.remove(output_path)
                except Exception:
                    pass

        pages_in_current_part = 0
        part_number += 1
        gc.collect()

    total_files = len(file_ids)

    try:
        with tempfile.TemporaryDirectory(prefix="maestro_compilar_pesado_") as temp_dir:
            for index, file_id in enumerate(file_ids, start=1):
                source_path = None
                source_doc = None
                try:
                    source_path = download_drive_file_to_path(service, str(file_id), temp_dir)
                    source_doc = fitz.open(source_path)

                    if source_doc.needs_pass:
                        # Igual que el flujo normal: solo intentamos contraseña vacía.
                        if not source_doc.authenticate(""):
                            raise ValueError(f"El PDF {file_id} está protegido con contraseña.")

                    total_pages_source = source_doc.page_count
                    start_page = 0

                    while start_page < total_pages_source:
                        available = page_limit - pages_in_current_part
                        if available <= 0:
                            flush_current(temp_dir, is_split=True)
                            available = page_limit

                        take = min(available, total_pages_source - start_page)
                        end_page = start_page + take - 1

                        # insert_pdf copia objetos PDF sin rasterizar las páginas.
                        output_doc.insert_pdf(
                            source_doc,
                            from_page=start_page,
                            to_page=end_page,
                        )

                        pages_in_current_part += take
                        start_page += take

                        if pages_in_current_part >= page_limit:
                            flush_current(temp_dir, is_split=True)

                    report(
                        processed_files=index,
                        total_files=total_files,
                        parts_created=len(parts),
                        pages_in_current_part=pages_in_current_part,
                        current_file_id=str(file_id),
                        processing_mode="heavy_disk",
                    )

                except Exception as exc:
                    errors.append({"file_id": str(file_id), "detail": str(exc)})
                    report(
                        processed_files=index,
                        total_files=total_files,
                        parts_created=len(parts),
                        pages_in_current_part=pages_in_current_part,
                        current_file_id=str(file_id),
                        last_error=str(exc),
                        processing_mode="heavy_disk",
                    )
                    if strict_mode:
                        try:
                            output_doc.close()
                        except Exception:
                            pass
                        trash_file_ids(service, [p.get("id", "") for p in parts])
                        return [], errors
                finally:
                    if source_doc is not None:
                        try:
                            source_doc.close()
                        except Exception:
                            pass
                    if source_path and os.path.exists(source_path):
                        try:
                            os.remove(source_path)
                        except Exception:
                            pass
                    gc.collect()

            flush_current(temp_dir, is_split=bool(parts))
            return parts, errors
    finally:
        try:
            output_doc.close()
        except Exception:
            pass
        gc.collect()



def compile_in_parts_heavy_qpdf(
    service, file_ids: list[str], destination_folder_id: str,
    output_filename: str, page_limit: int,
    progress_callback: ProgressCallback | None = None,
    replace_existing: bool = False, strict_mode: bool = True,
    expected_source_count: int = 0,
) -> tuple[list[dict[str, Any]], list[dict[str, str]]]:
    """PDFs gigantes: qpdf como proceso externo; no se cargan páginas en Python.

    Descarga a disco con la función existente y conserva el orden y la división
    por límite de páginas. No modifica el flujo normal ni las rutas HTTP.
    """
    qpdf_bin = shutil.which("qpdf")
    if not qpdf_bin:
        raise RuntimeError(
            "Falta qpdf en Railway. Agrega el Dockerfile del PASO 2 en la raíz "
            "del repositorio y despliega de nuevo."
        )
    if page_limit < 1:
        raise ValueError("El límite de páginas debe ser mayor que cero.")
    if expected_source_count and len(file_ids) != expected_source_count:
        raise ValueError("La cantidad de fuentes recibidas no coincide con la esperada.")

    def run_qpdf(args: list[str], timeout: int = 3600) -> str:
        # PASO 4: diagnósticos de la llamada a qpdf SIN modificar sus argumentos,
        # la fusión, las carátulas ni el flujo normal. Los contadores de cgroup
        # permiten detectar un OOM que no se vea en la gráfica de Railway.
        def memory_snapshot() -> dict[str, Any]:
            root = "/sys/fs/cgroup"
            snapshot: dict[str, Any] = {}
            for name in ("memory.current", "memory.peak", "memory.max"):
                try:
                    with open(os.path.join(root, name), "r", encoding="ascii") as f:
                        raw = f.read().strip()
                    snapshot[name] = int(raw) if raw.isdigit() else raw
                except (OSError, ValueError):
                    pass
            try:
                with open(os.path.join(root, "memory.events"), "r", encoding="ascii") as f:
                    snapshot["memory.events"] = {
                        key: int(value)
                        for line in f
                        for key, value in [line.strip().split()]
                    }
            except (OSError, ValueError):
                pass
            try:
                snapshot["tmp_free_mb"] = round(
                    shutil.disk_usage(tempfile.gettempdir()).free / (1024 * 1024), 1
                )
            except OSError:
                pass
            return snapshot

        before = memory_snapshot()
        operation = "fusionar" if "--pages" in args else "contar_paginas"
        app.logger.warning(
            "DIAG QPDF INICIO operacion=%s; memoria_y_disco=%s",
            operation, before,
        )
        started = time.monotonic()
        try:
            result = subprocess.run(
                [qpdf_bin, *args], capture_output=True, text=True,
                check=False, timeout=timeout,
            )
        except subprocess.TimeoutExpired as exc:
            app.logger.error(
                "DIAG QPDF TIMEOUT operacion=%s segundos=%.1f despues=%s",
                operation, time.monotonic() - started, memory_snapshot(),
            )
            raise RuntimeError(f"qpdf agotó el tiempo de {timeout}s") from exc

        after = memory_snapshot()
        before_events = before.get("memory.events", {})
        after_events = after.get("memory.events", {})
        oom_delta = int(after_events.get("oom", 0)) - int(before_events.get("oom", 0))
        killed_delta = int(after_events.get("oom_kill", 0)) - int(before_events.get("oom_kill", 0))
        app.logger.warning(
            "DIAG QPDF FIN operacion=%s rc=%s segundos=%.1f oom_delta=%s "
            "oom_kill_delta=%s; antes=%s; despues=%s",
            operation, result.returncode, time.monotonic() - started,
            oom_delta, killed_delta, before, after,
        )

        # qpdf: 0 = correcto; 3 = advertencias recuperables; 2 = error real.
        # Los códigos negativos son señales: -9 = SIGKILL, NUNCA éxito.
        if result.returncode == 3:
            detail = (result.stderr or "Advertencias sin detalle").strip()
            app.logger.warning("qpdf recuperó un PDF con advertencias: %s", detail[:800])
        elif result.returncode != 0:
            detail = (result.stderr or result.stdout or "error no detallado").strip()
            if result.returncode == -9:
                diagnosis = (
                    "cgroup registró OOM-kill durante esta operación"
                    if killed_delta > 0 else
                    "sin aumento de OOM-kill en este cgroup; revisar memoria, disco y eventos externos"
                )
                detail = f"SIGKILL (-9); {diagnosis}. {detail}"
            raise RuntimeError(
                f"qpdf terminó con código {result.returncode}: {detail[:800]}"
            )
        return result.stdout.strip()

    def page_count(path: str) -> int:
        result = run_qpdf(["--show-npages", path], timeout=600)
        try:
            count = int(result.splitlines()[-1].strip())
        except (ValueError, IndexError) as exc:
            raise ValueError(f"qpdf no devolvió páginas válidas para {os.path.basename(path)}") from exc
        if count < 1:
            raise ValueError("El PDF no contiene páginas.")
        return count

    base_name, extension = split_pdf_extension(output_filename)
    parts: list[dict[str, Any]] = []
    errors: list[dict[str, str]] = []
    segments: list[tuple[str, int, int]] = []
    pages_in_current_part = 0
    part_number = 1

    def report(**changes: Any) -> None:
        if progress_callback:
            progress_callback(**changes)

    def flush_current(temp_dir: str, preserve_path: str | None = None,
                      is_split: bool = False) -> None:
        nonlocal segments, pages_in_current_part, part_number
        if not segments:
            return
        count_expected = pages_in_current_part
        if is_split or part_number > 1:
            name = f"{base_name} (Parte {part_number}) ({count_expected} páginas){extension}"
        else:
            name = f"{base_name} ({count_expected} páginas){extension}"

        output_path = os.path.join(temp_dir, f"qpdf_compilado_{uuid.uuid4().hex}.pdf")
        sources_to_clean = {path for path, _, _ in segments}
        cmd = ["--empty", "--pages"]
        for path, start, end in segments:
            pages = str(start) if start == end else f"{start}-{end}"
            cmd.extend((path, pages))
        cmd.extend(("--", output_path))

        try:
            run_qpdf(cmd)
            if not os.path.isfile(output_path) or os.path.getsize(output_path) == 0:
                raise RuntimeError("qpdf no generó un PDF válido en disco.")
            actual_pages = page_count(output_path)
            if actual_pages != count_expected:
                raise RuntimeError(
                    f"Integridad: se esperaban {count_expected} páginas y qpdf generó {actual_pages}."
                )
            uploaded = upload_pdf_path(
                service, output_path, destination_folder_id, name,
                replace_existing=replace_existing,
            )
            uploaded["paginas"] = count_expected
            parts.append(uploaded)
            report(
                parts_created=len(parts), pages_in_current_part=0,
                last_created_file=uploaded.get("final_name", name),
                processing_mode="heavy_qpdf",
            )
        finally:
            try:
                if os.path.exists(output_path):
                    os.remove(output_path)
            except OSError:
                app.logger.warning("No se pudo eliminar temporal de qpdf: %s", output_path)

        # Sólo después de confirmar la subida: liberar las fuentes ya utilizadas.
        # Si un PDF ocupa más de una parte, se conserva hasta acabar sus páginas.
        for old_path in sources_to_clean:
            if old_path != preserve_path:
                try:
                    os.remove(old_path)
                except FileNotFoundError:
                    pass
        segments = []
        pages_in_current_part = 0
        part_number += 1

    try:
        with tempfile.TemporaryDirectory(prefix="maestro_qpdf_pesado_") as temp_dir:
            for index, file_id in enumerate(file_ids, start=1):
                source_path = None
                try:
                    source_path = download_drive_file_to_path(service, str(file_id), temp_dir)
                    num_pages = page_count(source_path)
                    next_page = 1
                    while next_page <= num_pages:
                        available = page_limit - pages_in_current_part
                        if available <= 0:
                            flush_current(temp_dir, preserve_path=source_path, is_split=True)
                            available = page_limit
                        end_page = min(num_pages, next_page + available - 1)
                        segments.append((source_path, next_page, end_page))
                        pages_in_current_part += end_page - next_page + 1
                        next_page = end_page + 1
                        if pages_in_current_part == page_limit:
                            keep = source_path if next_page <= num_pages else None
                            flush_current(temp_dir, preserve_path=keep, is_split=True)

                    # Si el último segmento ya se subió, no hace falta
                    # conservar esta fuente hasta el final del trabajo.
                    if source_path and not any(seg[0] == source_path for seg in segments):
                        if os.path.exists(source_path):
                            os.remove(source_path)

                    report(
                        processed_files=index, total_files=len(file_ids),
                        parts_created=len(parts),
                        pages_in_current_part=pages_in_current_part,
                        current_file_id=str(file_id),
                        processing_mode="heavy_qpdf",
                    )
                except Exception as exc:
                    # Un error al construir/subir una parte no permite ignorar
                    # sólo una fuente sin arriesgar un PDF incompleto.
                    # Fallar y revertir es preferible a reportar éxito parcial.
                    if segments or parts:
                        raise
                    errors.append({"file_id": str(file_id), "detail": str(exc)})
                    report(
                        processed_files=index, total_files=len(file_ids),
                        parts_created=len(parts),
                        pages_in_current_part=pages_in_current_part,
                        current_file_id=str(file_id), last_error=str(exc),
                        processing_mode="heavy_qpdf",
                    )
                    if strict_mode:
                        return [], errors
                finally:
                    if source_path and not any(seg[0] == source_path for seg in segments):
                        try:
                            if os.path.exists(source_path):
                                os.remove(source_path)
                        except OSError:
                            pass
            flush_current(temp_dir, is_split=bool(parts))
            return parts, errors
    except Exception:
        if parts:
            trash_file_ids(service, [p.get("id", "") for p in parts])
        raise

def compile_in_parts(
    service, file_ids: list[str], destination_folder_id: str,
    output_filename: str, page_limit: int,
    progress_callback: ProgressCallback | None = None,
    replace_existing: bool = False, strict_mode: bool = True,
    expected_source_count: int = 0,
) -> tuple[list[dict[str, Any]], list[dict[str, str]]]:
    if page_limit < 1:
        raise ValueError("El límite de páginas debe ser mayor que cero.")
    if expected_source_count and len(file_ids) != expected_source_count:
        raise ValueError("La cantidad de fuentes recibidas no coincide con la esperada.")

    # Detectar PDF gigante sin descargarlo. Si aparece uno, usamos la ruta
    # especial de bajo consumo de RAM. El flujo normal queda intacto.
    heavy_sources: list[tuple[str, int, str]] = []
    for _file_id in file_ids:
        try:
            _size, _name = get_drive_pdf_size(service, str(_file_id))
            if _size >= HEAVY_PDF_THRESHOLD_BYTES:
                heavy_sources.append((str(_file_id), _size, _name))
        except Exception as _meta_exc:
            # Si Drive no entrega el tamaño, no rompemos el comportamiento actual.
            app.logger.warning("No se pudo leer tamaño de %s: %s", _file_id, _meta_exc)

    if heavy_sources:
        app.logger.warning(
            "Modo PDF gigante activado. Umbral=%s MB | Fuentes=%s",
            HEAVY_PDF_THRESHOLD_MB,
            [f"{name} ({size / (1024 * 1024):.1f} MB)" for _, size, name in heavy_sources],
        )
        if progress_callback:
            progress_callback(
                processing_mode="heavy_qpdf",
                heavy_files=len(heavy_sources),
                heavy_threshold_mb=HEAVY_PDF_THRESHOLD_MB,
            )
        return compile_in_parts_heavy_qpdf(
            service=service,
            file_ids=file_ids,
            destination_folder_id=destination_folder_id,
            output_filename=output_filename,
            page_limit=page_limit,
            progress_callback=progress_callback,
            replace_existing=replace_existing,
            strict_mode=strict_mode,
            expected_source_count=expected_source_count,
        )

    base_name, extension = split_pdf_extension(output_filename)
    parts: list[dict[str, Any]] = []
    errors: list[dict[str, str]] = []
    writer = PdfWriter()
    pages_in_current_part = 0
    part_number = 1

    def report(**changes: Any) -> None:
        if progress_callback:
            progress_callback(**changes)

    with tempfile.TemporaryDirectory(prefix="maestro_compilar_") as temp_dir:
        def flush_current(is_split: bool) -> None:
            nonlocal writer, pages_in_current_part, part_number
            if pages_in_current_part == 0:
                return
            if is_split or part_number > 1:
                part_name = f"{base_name} (Parte {part_number}) ({pages_in_current_part} páginas){extension}"
            else:
                part_name = f"{base_name} ({pages_in_current_part} páginas){extension}"

            output_path = write_pdf_to_path(writer, temp_dir, "compilado")
            uploaded = upload_pdf_path(service, output_path, destination_folder_id, part_name, replace_existing=replace_existing)
            uploaded["paginas"] = pages_in_current_part
            parts.append(uploaded)
            report(parts_created=len(parts), pages_in_current_part=0, last_created_file=uploaded.get("final_name", part_name))

            if os.path.exists(output_path):
                try:
                    os.remove(output_path)
                except Exception:
                    pass

            writer = PdfWriter()
            pages_in_current_part = 0
            part_number += 1
            # Liberar referencias del writer al cerrar cada parte, pero evitar
            # forzar GC después de cada PDF (coste de CPU innecesario).
            gc.collect()

        total_files = len(file_ids)
        for index, file_id in enumerate(file_ids, start=1):
            source_handle = None
            source_path = None
            try:
                source_path = download_drive_file_to_path(service, str(file_id), temp_dir)
                source_handle, reader = open_pdf_reader_path(source_path, str(file_id))

                for page in reader.pages:
                    if pages_in_current_part >= page_limit:
                        flush_current(is_split=True)
                    writer.add_page(page)
                    pages_in_current_part += 1

                report(processed_files=index, total_files=total_files, parts_created=len(parts),
                       pages_in_current_part=pages_in_current_part, current_file_id=str(file_id))
            except Exception as exc:
                errors.append({"file_id": str(file_id), "detail": str(exc)})
                report(processed_files=index, total_files=total_files, parts_created=len(parts),
                       pages_in_current_part=pages_in_current_part, current_file_id=str(file_id), last_error=str(exc))
                if strict_mode:
                    trash_file_ids(service, [p.get("id", "") for p in parts])
                    return [], errors
            finally:
                if source_handle:
                    try:
                        source_handle.close()
                    except Exception:
                        pass
                if source_path and os.path.exists(source_path):
                    try:
                        os.remove(source_path)
                    except Exception:
                        pass

            # Forzar GC solo por lotes. Los objetos temporales del lector ya no
            # necesitan una recolección completa después de cada archivo.
            if index % GC_COLLECT_EVERY_FILES == 0:
                gc.collect()

        flush_current(is_split=bool(parts))
        return parts, errors


def assemble_tomo(
    service, source_ids: list[str], destination_folder_id: str,
    output_filename: str, progress_callback: ProgressCallback | None = None,
    replace_existing: bool = False, strict_mode: bool = True,
    expected_pages: int = 0, expected_source_count: int = 0,
) -> tuple[dict[str, Any] | None, int, list[dict[str, str]]]:
    if expected_source_count and len(source_ids) != expected_source_count:
        return None, 0, [{"file_id": "", "detail": "La cantidad de fuentes recibidas no coincide con la esperada."}]

    writer = PdfWriter()
    errors: list[dict[str, str]] = []
    total_pages = 0
    total_files = len(source_ids)

    def report(**changes: Any) -> None:
        if progress_callback:
            progress_callback(**changes)

    with tempfile.TemporaryDirectory(prefix="maestro_tomo_") as temp_dir:
        for index, file_id in enumerate(source_ids, start=1):
            source_handle = None
            source_path = None
            try:
                source_path = download_drive_file_to_path(service, file_id, temp_dir)
                source_handle, reader = open_pdf_reader_path(source_path, file_id)
                for page in reader.pages:
                    writer.add_page(page)
                    total_pages += 1
                report(processed_files=index, total_files=total_files, pages_in_current_part=total_pages, current_file_id=file_id)
            except Exception as exc:
                errors.append({"file_id": file_id, "detail": str(exc)})
                report(processed_files=index, total_files=total_files, pages_in_current_part=total_pages, current_file_id=file_id, last_error=str(exc))
                if strict_mode:
                    return None, total_pages, errors
            finally:
                if source_handle:
                    try:
                        source_handle.close()
                    except Exception:
                        pass
                if source_path and os.path.exists(source_path):
                    try:
                        os.remove(source_path)
                    except Exception:
                        pass

            if index % GC_COLLECT_EVERY_FILES == 0:
                gc.collect()

        if total_pages == 0:
            return None, 0, errors

        if expected_pages and total_pages != expected_pages:
            errors.append({"file_id": "", "detail": f"Integridad: se esperaban {expected_pages} páginas y se obtuvieron {total_pages}."})
            return None, total_pages, errors

        output_path = write_pdf_to_path(writer, temp_dir, "tomo")
        uploaded = upload_pdf_path(service, output_path, destination_folder_id, output_filename, replace_existing=replace_existing)
        uploaded["paginas"] = total_pages

        if os.path.exists(output_path):
            try:
                os.remove(output_path)
            except Exception:
                pass

        gc.collect()
        return uploaded, total_pages, errors


def run_background_job(job_id: str, job_type: str, token: str, payload: dict[str, Any]) -> None:
    update_job(job_id, job_state="running", status="running")
    try:
        with job_semaphore:
            service = get_drive_service(token)
            def progress_callback(**changes: Any) -> None:
                update_job_progress(job_id, **changes)

            if job_type == "compilar":
                file_ids = [str(value).strip() for value in payload["file_ids"] if str(value).strip()]
                page_limit = int(payload.get("limite_paginas", 500))
                parts, errors = compile_in_parts(
                    service=service, file_ids=file_ids,
                    destination_folder_id=str(payload["destination_folder_id"]),
                    output_filename=str(payload["output_filename"]),
                    page_limit=page_limit, progress_callback=progress_callback,
                    replace_existing=as_bool(payload.get("replace_existing")),
                    strict_mode=as_bool(payload.get("strict_mode", True)),
                    expected_source_count=int(payload.get("expected_source_count") or 0),
                )

                if not parts:
                    update_job(job_id, job_state="failed", status="error",
                               detail=(errors[-1].get("detail") if errors else "No se pudo generar ningún PDF."),
                               result={"partes": [], "errores": errors})
                    return

                final_status = "partial" if errors else "success"
                update_job(job_id, job_state="finished", status=final_status,
                           result={"status": final_status, "message": "Compilación terminada.", "partes": parts, "errores": errors})
                return

            if job_type == "tomos":
                source_ids: list[str] = []
                caratula_id = str(payload.get("caratula_id") or "").strip()
                if caratula_id:
                    source_ids.append(caratula_id)
                source_ids.extend(str(value).strip() for value in payload["file_ids"] if str(value).strip())

                uploaded, total_pages, errors = assemble_tomo(
                    service=service, source_ids=source_ids,
                    destination_folder_id=str(payload["destination_folder_id"]),
                    output_filename=str(payload["output_filename"]),
                    progress_callback=progress_callback,
                    replace_existing=as_bool(payload.get("replace_existing")),
                    strict_mode=as_bool(payload.get("strict_mode", True)),
                    expected_pages=int(payload.get("expected_pages") or 0),
                    expected_source_count=int(payload.get("expected_source_count") or 0),
                )

                if not uploaded:
                    update_job(job_id, job_state="failed", status="error",
                               detail=(errors[-1].get("detail") if errors else "No se pudo leer ninguna página."),
                               result={"errores": errors})
                    return

                final_status = "partial" if errors else "success"
                update_job(job_id, job_state="finished", status=final_status,
                           result={"status": final_status, "message": "Tomo ensamblado.", "id": uploaded["id"],
                                   "url": uploaded["url"], "final_name": uploaded["final_name"],
                                   "paginas": total_pages, "errores": errors})
                return

    except Exception as exc:
        app.logger.exception("Error crítico en trabajo %s", job_id)
        update_job(job_id, job_state="failed", status="error", detail=str(exc))
    finally:
        gc.collect()


# ================================================================
# RUTAS HTTP
# ================================================================

from panel_routes import panel_bp  # noqa: E402  (registrado luego de definir `app`)
app.register_blueprint(panel_bp)


@app.get("/health")
def health():
    cleanup_expired_jobs()
    with jobs_lock:
        active_jobs = sum(1 for job in jobs.values() if job.get("job_state") in {"queued", "running"})
    return jsonify({"status": "ok", "service": "sistema-maestro-pdf", "max_concurrent_jobs": MAX_CONCURRENT_JOBS, "active_jobs": active_jobs}), 200


@app.get("/trabajos/<job_id>")
def consultar_trabajo(job_id: str):
    try:
        get_bearer_token()
        public_job = get_public_job(str(job_id).strip())
        if not public_job:
            return jsonify({"status": "error", "job_state": "not_found", "detail": "Trabajo no encontrado o vencido."}), 404
        return jsonify(public_job), 200
    except ValueError as exc:
        return jsonify({"status": "error", "detail": str(exc)}), 400
    except Exception as exc:
        app.logger.exception("Error consultando trabajo %s", job_id)
        return jsonify({"status": "error", "detail": str(exc)}), 500


@app.post("/compilar")
def compilar_general():
    try:
        data = validate_payload(request.get_json(silent=True), ["file_ids", "destination_folder_id", "output_filename"])
        token = get_bearer_token()
        file_ids = [str(value).strip() for value in data["file_ids"] if str(value).strip()]

        if not file_ids:
            raise ValueError("La lista file_ids está vacía.")
        if len(file_ids) > 3000:
            raise ValueError("La solicitud supera el máximo de 3000 archivos.")

        data["file_ids"] = file_ids
        data["limite_paginas"] = int(data.get("limite_paginas", 500))

        if as_bool(data.get("modo_async")):
            request_id = str(data.get("request_id") or uuid.uuid4().hex).strip()
            job_id, created = create_or_reuse_job(job_type="compilar", request_id=request_id, token=token, payload=data)
            return jsonify({"status": "accepted", "job_state": "queued", "job_id": job_id, "request_id": request_id,
                            "reused": not created, "poll_url": f"/trabajos/{job_id}", "message": "Trabajo recibido."}), 202

        service = get_drive_service(token)
        with job_semaphore:
            parts, errors = compile_in_parts(
                service=service, file_ids=file_ids, destination_folder_id=str(data["destination_folder_id"]),
                output_filename=str(data["output_filename"]), page_limit=int(data.get("limite_paginas", 500)),
                replace_existing=as_bool(data.get("replace_existing")), strict_mode=as_bool(data.get("strict_mode", True)),
                expected_source_count=int(data.get("expected_source_count") or 0),
            )

        if not parts:
            return jsonify({"status": "error", "detail": (errors[-1].get("detail") if errors else "No se pudo generar ningún PDF."), "errores": errors}), 422
        return jsonify({"status": "partial" if errors else "success", "message": "Compilación terminada.", "partes": parts, "errores": errors}), 200

    except ValueError as exc:
        return jsonify({"status": "error", "detail": str(exc)}), 400
    except Exception as exc:
        app.logger.exception("Error crítico en /compilar")
        return jsonify({"status": "error", "detail": str(exc)}), 500


@app.post("/tomos")
def ensamblar_tomo():
    try:
        data = validate_payload(request.get_json(silent=True), ["file_ids", "destination_folder_id", "output_filename"])
        token = get_bearer_token()
        data["file_ids"] = [str(value).strip() for value in data["file_ids"] if str(value).strip()]

        if as_bool(data.get("modo_async")):
            request_id = str(data.get("request_id") or uuid.uuid4().hex).strip()
            job_id, created = create_or_reuse_job(job_type="tomos", request_id=request_id, token=token, payload=data)
            return jsonify({"status": "accepted", "job_state": "queued", "job_id": job_id, "request_id": request_id,
                            "reused": not created, "poll_url": f"/trabajos/{job_id}", "message": "Trabajo de tomo recibido."}), 202

        source_ids: list[str] = []
        caratula_id = str(data.get("caratula_id") or "").strip()
        if caratula_id:
            source_ids.append(caratula_id)
        source_ids.extend(data["file_ids"])

        if not source_ids:
            raise ValueError("No hay archivos para ensamblar.")

        service = get_drive_service(token)
        with job_semaphore:
            uploaded, total_pages, errors = assemble_tomo(
                service=service, source_ids=source_ids, destination_folder_id=str(data["destination_folder_id"]),
                output_filename=str(data["output_filename"]), replace_existing=as_bool(data.get("replace_existing")),
                strict_mode=as_bool(data.get("strict_mode", True)), expected_pages=int(data.get("expected_pages") or 0),
                expected_source_count=int(data.get("expected_source_count") or 0),
            )

        if not uploaded:
            return jsonify({"status": "error", "detail": (errors[-1].get("detail") if errors else "No se pudo leer ninguna página."), "errores": errors}), 422
        return jsonify({"status": "partial" if errors else "success", "message": "Tomo ensamblado.", "id": uploaded["id"],
                        "url": uploaded["url"], "final_name": uploaded["final_name"], "paginas": total_pages, "errores": errors}), 200

    except ValueError as exc:
        return jsonify({"status": "error", "detail": str(exc)}), 400
    except Exception as exc:
        app.logger.exception("Error crítico en /tomos")
        return jsonify({"status": "error", "detail": str(exc)}), 500


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=PORT)
