# Crear y usar Dataflows (Gen2) en Microsoft Fabric

En Microsoft Fabric, los **Dataflows (Gen2)** se conectan a diversas fuentes de datos y realizan transformaciones en Power Query Online. Luego, pueden utilizarse en Data Pipelines para ingerir datos en un Lakehouse u otro almacén analítico, o para definir un conjunto de datos para un informe de Power BI.

Este laboratorio está diseñado para introducir los diferentes elementos de Dataflows (Gen2) y no para crear una solución compleja que pueda existir en una empresa. Este laboratorio toma aproximadamente **30 minutos** en completarse.

---

## Requisitos previos
Necesitas acceso a una capacidad de Fabric de pago o de prueba (Trial) para completar este ejercicio. Para obtener información sobre la prueba gratuita de Fabric, consulta la documentación oficial de Microsoft.

---

## 1. Crear un área de trabajo (Workspace)

1. Navega a la página de inicio de Microsoft Fabric en un navegador: `https://app.fabric.microsoft.com/home?experience=fabric` e inicia sesión con tus credenciales.
2. En la barra de menú de la izquierda, selecciona **Workspaces** (el ícono se asemeja a una carpeta 🗇).
3. Crea un nuevo workspace con el nombre de tu elección, seleccionando un modo de licencia que incluya capacidad de Fabric (Trial, Premium o Fabric).
4. Cuando tu nuevo workspace se abra, debería estar vacío.

## 2. Crear un Lakehouse

Una vez que tengas el workspace, es momento de crear un Lakehouse de datos en el cual ingerirás los datos.

1. En la barra de menú de la izquierda, selecciona **Create**.
2. En la página de **New**, bajo la sección **Data Engineering**, selecciona **Lakehouse**.
3. Asígnale un nombre único de tu elección. En las imágenes de ejemplo de este laboratorio se utiliza el nombre **`dataflowLH`**.
4. Después de aproximadamente un minuto, se creará un nuevo Lakehouse vacío.

## 3. Crear un Dataflow (Gen2) para ingerir datos

Ahora que tienes un Lakehouse, necesitas ingerir datos en él. Una forma de hacerlo es definir un Dataflow que encapsule un proceso de extracción, transformación y carga (ETL).

1. En la página de inicio de tu Lakehouse, selecciona el botón **Get data** y luego elige **New Dataflow Gen2**.
   *(Observa la **Imagen 1** y la **Imagen 2** para ver la ubicación exacta de esta opción en la interfaz).*
2. Se abrirá el editor de Power Query para el nuevo Dataflow. En el menú de inicio, selecciona **Import from a Text/CSV file**.
   *(Observa la **Imagen 3**).*
3. Crea una nueva conexión con la siguiente configuración:
   * **Link to file:** Seleccionado.
   * **File path or URL:** `https://raw.githubusercontent.com/MicrosoftLearning/dp-data/main/orders.csv`
   * **Connection:** Create new connection.
   * **Connection name:** Especifica un nombre único, por ejemplo `connection1`.
   * **Data gateway:** (ninguno).
   * **Authentication kind:** Anonymous.
   *(Observa la **Imagen 4**).*
4. Selecciona **Next** para obtener una vista previa de los datos del archivo y luego haz clic en **Create** para confirmar la fuente de datos.
   *(Observa la **Imagen 5** para ver la vista previa de los datos).*

## 4. Transformaciones en el editor Power Query

El editor de Power Query mostrará la fuente de datos y un conjunto inicial de pasos de consulta para dar formato a los datos.

### Crear una columna personalizada (Custom Column)

1. En la cinta de opciones de la barra de herramientas, selecciona la pestaña **Add column**.
2. Luego selecciona **Custom column** para crear una nueva columna.
   *(Observa la **Imagen 6**).*
3. En el panel de configuración de la columna personalizada:
   * **New column name:** `MonthNo`
   * **Data type:** Whole number
   * **Custom column formula:** `Date.Month([OrderDate])`
   *(Observa la **Imagen 7**).*
4. Haz clic en **OK** para crear la columna.
5. Nota cómo el paso para agregar la columna personalizada se añade a la consulta (en el panel de la derecha, bajo "Applied steps"). La columna resultante se muestra en el panel de datos.
   *(Observa la **Imagen 8**, el resultado es una nueva columna `MonthNo` con valores numéricos de mes).*

### Cambiar tipos de datos

1. Verifica y confirma que el tipo de datos para la columna **`OrderDate`** esté configurado como **Date**.
   * Selecciona la columna `OrderDate` y cambia su tipo de datos a Date en la pestaña **Transform** si no lo está ya.
   *(Observa la **Imagen 9** donde se selecciona `OrderDate` y se cambia su tipo de datos a `Date` usando el menú desplegable).*
2. Confirma que la columna recién creada `MonthNo` tenga el tipo de datos **Whole Number**.

### Nota adicional sobre el Editor Power Query
En el panel de configuración de consultas (Query Settings) a la derecha, observa que los **Applied Steps** incluyen cada paso de transformación. Estos pasos se pueden mover hacia arriba o hacia abajo, editar seleccionando el ícono de engranaje (⚙️), y puedes seleccionar cada paso para ver cómo se aplican las transformaciones en el panel de vista previa. También puedes activar el **Diagram flow** para obtener una vista visual del diagrama de los pasos.

## 📸 Imágenes del desarrollo del laboratorio

A continuación se listan las capturas adjuntas que ilustran el proceso:

*   **Imagen 1:** Acceso a Dataflows Gen2 desde el menú lateral izquierdo y creación del Dataflow desde el Lakehouse (`dataflowLH`).
*   **Imagen 2:** Menú desplegable "Get data" dentro del Lakehouse seleccionando la opción "New Dataflow Gen2".
*   **Imagen 3:** Interfaz del editor de Power Query, selección de la fuente de datos "Import from a Text/CSV file".
*   **Imagen 4:** Configuración de credenciales de conexión (URL del CSV, autenticación Anónima).
*   **Imagen 5:** Panel de previsualización de datos antes de crear la consulta.
*   **Imagen 6:** Selección de la opción "Custom column" dentro de la pestaña "Add column" de Power Query.
*   **Imagen 7:** Configuración de la columna personalizada: nombre `MonthNo`, tipo `Whole number` y fórmula `Date.Month([OrderDate])`.
*   **Imagen 8:** Resultado final en el editor, con la nueva columna `MonthNo` y los pasos aplicados visibles en el panel derecho.
*   **Imagen 9:** Cambio del tipo de datos de la columna `OrderDate` a `Date` desde la pestaña "Transform".