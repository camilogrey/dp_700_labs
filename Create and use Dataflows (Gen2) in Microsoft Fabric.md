# Laboratorio: Creación y uso de Dataflows (Gen2) en Microsoft Fabric

### 1. Creación del workspace
Accedí a Microsoft Fabric mediante el navegador e inicié sesión con mis credenciales. En la barra lateral izquierda, seleccioné el icono de **Workspaces** y creé un nuevo workspace con un nombre de mi elección, asegurándome de seleccionar un modo de licencia que incluyera capacidad de Fabric (usé la opción de prueba). El workspace quedó vacío, listo para comenzar.

### 2. Creación del lakehouse
Desde el workspace, en la barra lateral izquierda, seleccioné **Create** y, dentro de la sección **Data Engineering**, elegí **Lakehouse**. Asigné el nombre **`dataflowLH`** y esperé unos minutos hasta que se creara. El lakehouse apareció con la estructura de carpetas **Tables** y **Files**, listo para recibir datos.

> ![Lakehouse recién creado](img_dataflowgen2/1.%20New%20Lakehouse%20inside%20workspace.png)

### 3. Inicio de la creación del Dataflow (Gen2)
En la página principal del lakehouse, desde el menú **Get data**, seleccioné **New Dataflow Gen2**. Tras unos segundos, se abrió el editor de Power Query, donde comenzaría a definir el proceso de extracción, transformación y carga (ETL).

> ![Opción New Dataflow Gen2 en el menú Get data](img_dataflowgen2/2.%20get%20data%20new%20dataflow%20gen2.png)

### 4. Importación de datos desde un archivo CSV
Dentro del editor de Power Query, en la ventana de inicio, elegí la opción **Import from a Text/CSV file** para conectar con el origen de datos.

> ![Selección de importación desde Text/CSV](img_dataflowgen2/3.%20In%20dataglow%20gen%202%20import%20csv.png)

A continuación, configuré la conexión con los siguientes parámetros:
- **File path or URL**: `https://raw.githubusercontent.com/MicrosoftLearning/dp-data/main/orders.csv`
- **Connection**: Creé una nueva conexión con el nombre `connection1`
- **Data gateway**: (ninguno)
- **Authentication kind**: `Anonymous`

> ![Configuración de la conexión al CSV](img_dataflowgen2/4.%20connect%20csv%20data%20with%20dataflow%20gen2.png)

Hice clic en **Next** para previsualizar los datos y verificar que se cargaran correctamente. La vista previa mostró las columnas esperadas (`SalesOrderID`, `OrderDate`, `CustomerID`, etc.).

> ![Vista previa de los datos del CSV](img_dataflowgen2/5.%20data%20preview.png)

Finalmente, seleccioné **Create** para que el dataflow importara los datos y generara la consulta base.

### 5. Adición de una columna personalizada
Para enriquecer los datos, necesitaba extraer el número de mes de la columna `OrderDate`. En la cinta de opciones del editor, fui a la pestaña **Add column** y seleccioné **Custom column**.

> ![Opción Custom column en el menú Add column](img_dataflowgen2/6.%20create%20a%20custom%20column%20in%20data%20table.png)

En el cuadro de diálogo, configuré:
- **New column name**: `MonthNo`
- **Data type**: `Whole number`
- **Custom column formula**: `= Date.Month([OrderDate])`

> ![Configuración de la columna personalizada](img_dataflowgen2/7.%20new%20columns%20with%20some%20parameters.png)

Al hacer clic en **OK**, la nueva columna se agregó a la tabla, y el paso correspondiente se registró en **Applied Steps** en el panel de configuración de la consulta.

> ![Tabla con la nueva columna MonthNo y los pasos aplicados](img_dataflowgen2/8.%20new%20column%20done.png)

### 6. Verificación y ajuste de tipos de datos
Para asegurar que las columnas tuvieran el tipo correcto, verifiqué que:
- La columna `OrderDate` estuviera configurada como tipo **Date**.
- La columna `MonthNo` estuviera configurada como tipo **Whole number**.

En el editor, seleccioné la columna `OrderDate` y, desde el menú desplegable de tipo de datos, elegí **Date**. De manera similar, confirmé que `MonthNo` tuviera el tipo **Whole number** (aunque la imagen se centra en `OrderDate`, el laboratorio indica que ambos deben revisarse).

> ![Cambio del tipo de datos de OrderDate a Date](img_dataflowgen2/9.%20change%20data%20type%20orderdate%20column.png)

Con estos pasos, el dataflow quedó configurado con la transformación necesaria. Los datos estaban listos para ser cargados en el lakehouse o utilizados en un pipeline posterior.

---

**Nota :** Este laboratorio introdujo los conceptos básicos de Dataflows Gen2, mostrando cómo conectar a un origen CSV, aplicar transformaciones (como la adición de una columna calculada) y gestionar los tipos de datos. El dataflow creado puede ahora ser utilizado en pipelines de datos o como origen para modelos semánticos.