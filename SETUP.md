# Puesta en marcha del Panel Maestro (versión web independiente)

Esto reemplaza el sidebar de Apps Script por una página que corre en el
mismo servicio Flask que ya tenías en Railway. El Google Sheet **sigue
siendo la fuente de verdad**: el Panel de Parámetros (links de C2:C8) y
los logs de "CARATULAS Y COMPILADOS" / "TOMOS" se leen y escriben igual
que antes, solo que ahora por API en vez de por Apps Script.

## 1. Crear la cuenta de servicio de Google

1. Ve a [Google Cloud Console](https://console.cloud.google.com/) → usa el
   mismo proyecto que ya tengas, o crea uno nuevo.
2. Habilita estas 4 APIs: **Google Sheets API**, **Google Drive API**,
   **Google Docs API**, **Google Slides API**.
3. Ve a *IAM y administración → Cuentas de servicio → Crear cuenta de servicio*.
   No necesita ningún rol de proyecto (los permisos reales se dan
   compartiendo el Sheet/Drive, ver paso 2).
4. Entra a la cuenta creada → *Claves → Agregar clave → Crear clave nueva → JSON*.
   Se descarga un archivo `.json`. Guárdalo, lo necesitas en el paso 3.
5. Copia el **email** de la cuenta de servicio (algo como
   `panel-maestro@tu-proyecto.iam.gserviceaccount.com`).

## 2. Compartir el Sheet y las carpetas de Drive

La cuenta de servicio necesita acceso **igual que un usuario más**:

1. Abre el Google Sheet del sistema → **Compartir** → agrega el email de
   la cuenta de servicio como **Editor**.
2. Comparte también, como Editor, **cada carpeta raíz** referenciada en
   las celdas del Panel de Parámetros (C2, C3, C4, C6 de "CARATULAS Y
   COMPILADOS"; C3, C4, C5, C6 de "TOMOS"). Al compartir una carpeta raíz,
   Drive da acceso automáticamente a todo lo que esté dentro.

Si algún link apunta a "Compartido conmigo" y no a una carpeta que sea
directamente tuya, muévela o vuelve a compartirla explícitamente con la
cuenta de servicio — las cuentas de servicio no heredan "Compartido conmigo".

## 3. Crear la pestaña USUARIOS (login propio)

En el mismo Sheet, crea una pestaña llamada exactamente **USUARIOS** con
estas columnas desde la fila 1:

| A (usuario) | B (password_hash) | C (rol)  | D (activo) |
|---|---|---|---|
| jadmin | (hash) | admin | TRUE |

Para generar el hash de una contraseña, en tu computadora con Python:

```bash
python3 -c "from werkzeug.security import generate_password_hash as g; print(g('la_contraseña_elegida'))"
```

Pega el resultado en la columna B. Nunca guardes la contraseña en texto plano.

## 4. Variables de entorno en Railway

Agrega estas variables al servicio (además de las que ya tenías):

| Variable | Valor |
|---|---|
| `SHEET_ID` | El ID del Google Sheet (la parte de la URL entre `/d/` y `/edit`) |
| `GOOGLE_SERVICE_ACCOUNT_JSON` | El contenido **completo** del archivo `.json` descargado en el paso 1, pegado como una sola línea/valor |
| `FLASK_SECRET_KEY` | Cualquier cadena larga y aleatoria, para firmar la cookie de sesión |
| `SISTEMA_MAESTRO_KEY` | (la que ya tenías, opcional) |

## 5. Archivos del proyecto

Reemplaza/agrega estos archivos en tu repo de Railway:

```
app.py                  (actualizado: registra el blueprint del panel)
panel_routes.py         (nuevo: rutas /api/*)
google_services.py      (nuevo: cliente Sheets/Drive/Docs/Slides)
auth_service.py         (nuevo: login contra la pestaña USUARIOS)
caratulas_service.py    (nuevo: puerto de 3_Caratulas.txt)
compilador_service.py   (nuevo: puerto de 4_Compilador.txt)
requirements.txt        (actualizado con google-auth y google-api-python-client)
templates/index.html    (actualizado: ya no usa google.script.run, usa fetch a /api/*)
static/styles.css       (el mismo que ya tenías)
```

## 6. Qué funciona ya y qué falta

✅ **Login** con usuario/contraseña propios (pestaña USUARIOS), queda
   registrado quién hace cada acción.
✅ **Panel de Parámetros**: la página lee C2:C8 en vivo desde el Sheet.
✅ **Carátulas**: generación desde plantilla (Docs/Slides), restauración
   de carátulas base de Anexos 11/13, creación de carpetas — igual que
   el original, escribiendo el log en la hoja "CARATULAS Y COMPILADOS".
✅ **Compilador**: arma la secuencia (reglas de Anexo 11/13 incluidas) y
   compila en el mismo servidor — ya no hace falta un servidor Colab
   aparte para esto, todo corre en Railway.
✅ **Historial**: la tabla inferior lee directamente las filas ya
   guardadas en el Sheet.

🚧 **Tomos**: el módulo de proyección/índices/fusión de tomos
   (`5_Tomos.txt`, 1554 líneas) todavía no está portado — es el
   siguiente paso. Mientras tanto la pestaña muestra un aviso.

### Simplificación conocida
El original centraba automáticamente ciertos textos dentro de la
carátula (alineación de párrafo). La versión Python reemplaza el texto
pero no reproduce ese centrado automático — si la plantilla ya tiene el
placeholder centrado por estilo, se ve igual; si no, hay que ajustarlo
en la plantilla una sola vez.
