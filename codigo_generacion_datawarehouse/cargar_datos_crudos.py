from google.cloud import bigquery
from google.api_core.exceptions import BadRequest, NotFound
from datetime import datetime, timedelta

def cargar_datos_de_gcs_a_bigquery(GCS_URI, ID_tabla, esquema_tabla):
    """
    Esta función carga datos de un bucket de Google Cloud Storage (GCS) a una tabla específica de BigQuery.

    Parameters:
    GCS_URI (str): El Identificador Uniforme de Recursos (URI) del bucket de GCS y el archivo a cargar.
    ID_tabla (str): El ID de la tabla de BigQuery donde se cargarán los datos.
    esquema_tabla (list): El esquema de la tabla de BigQuery.

    Returns:
    None

    Raises:
    BadRequest: Si la solicitud no es válida.
    NotFound: Si el bucket de GCS especificado o la tabla de BigQuery no se encuentran.
    """

    #Creo un objeto LoadJobConfig con el esquema especificado, formato de origen, disposición de escritura y filas de encabezado que omitir.
    job_config = bigquery.LoadJobConfig(
        schema=esquema_tabla,
        source_format=bigquery.SourceFormat.CSV,
        write_disposition = 'WRITE_APPEND',
        skip_leading_rows=1
    )

    #Cargo los datos del URI de GCS especificado a la tabla de BigQuery especificada utilizando el objeto LoadJobConfig.
    load_job = client.load_table_from_uri(GCS_URI, ID_tabla, job_config=job_config)

    #Espero a que se complete el trabajo de carga.
    load_job.result()

    #Obtengo los metadatos de la tabla actualizados.
    tabla = client.get_table(ID_tabla)

    #Voy imprimiendo el número de filas cargados a la tabla
    print(f"Se cargaron {tabla.num_rows} filas a la tabla {ID_tabla}")

#Defino los esquemas de cada tabla
esquema_tabla_stock = [
    bigquery.SchemaField("codigo_sucursal", "INTEGER"),
    bigquery.SchemaField("fecha_cierre_comercial", "DATETIME"),
    bigquery.SchemaField("SKU_codigo", "STRING"),
    bigquery.SchemaField("SKU_descripcion", "STRING"),
    bigquery.SchemaField("stock_unidades", "INTEGER"),
    bigquery.SchemaField("unidad", "STRING"),
    bigquery.SchemaField("n_distribuidor", "INTEGER"),
]

esquema_tabla_venta = [
    bigquery.SchemaField("codigo_sucursal", "INTEGER"),
    bigquery.SchemaField("codigo_cliente", "INTEGER"),
    bigquery.SchemaField("fecha_cierre_comercial", "DATETIME"),
    bigquery.SchemaField("SKU_codigo", "STRING"),
    bigquery.SchemaField("venta_unidades", "INTEGER"),
    bigquery.SchemaField("venta_importe", "FLOAT"),
    bigquery.SchemaField("condicion_venta", "STRING"),
    bigquery.SchemaField("n_distribuidor", "INTEGER"),
]

esquema_tabla_deuda = [
    bigquery.SchemaField("codigo_sucursal", "INTEGER"),
    bigquery.SchemaField("codigo_cliente", "INTEGER"),
    bigquery.SchemaField("fecha_cierre_comercial", "DATETIME"),
    bigquery.SchemaField("deuda_vencida", "FLOAT"),
    bigquery.SchemaField("deuda_total", "FLOAT"),
    bigquery.SchemaField("n_distribuidor", "INTEGER"),
]

esquema_tabla_cliente = [
    bigquery.SchemaField("codigo_sucursal", "INTEGER"),
    bigquery.SchemaField("codigo_cliente", "INTEGER"),
    bigquery.SchemaField("ciudad", "STRING"),
    bigquery.SchemaField("provincia", "STRING"),
    bigquery.SchemaField("estado", "STRING"),
    bigquery.SchemaField("nombre_cliente", "STRING"),
    bigquery.SchemaField("cuit", "INTEGER"),
    bigquery.SchemaField("razon_social", "STRING"),
    bigquery.SchemaField("direccion", "STRING"),
    bigquery.SchemaField("dias_visita", "STRING"),
    bigquery.SchemaField("telefono", "STRING"),
    bigquery.SchemaField("fecha_alta", "DATETIME"),
    bigquery.SchemaField("fecha_baja", "DATETIME", mode="NULLABLE"),
    bigquery.SchemaField("lat", "FLOAT"),
    bigquery.SchemaField("long", "FLOAT"),
    bigquery.SchemaField("condicion_venta", "STRING"),
    bigquery.SchemaField("deuda_vencida", "FLOAT"),
    bigquery.SchemaField("tipo_negocio", "STRING"),
    bigquery.SchemaField("n_distribuidor", "INTEGER"),
]

#Hago la conexión con el cliente, importante pasarle el ID del proyecto como argumento
client = bigquery.Client("usm-infra-grupo8-401213")

if __name__ == '__main__':
    cant_dist = 4 # Aquí la cantidad siempre debe ser una más que la real
    cant_dias = 2
    ID_proyecto = "usm-infra-grupo8-401213"
    ruta_base = "gs://datos_argideal_grupo8"
    lista_tablas = ['stock', 'venta', 'deuda', 'cliente']
    fecha_actual = datetime.now()
    
    for distribuidor in range(1, cant_dist):
        print(f"\n<Distribuidor ({distribuidor})>")
        for d in range(0, cant_dias):
            try:
                fecha_cierre = fecha_actual - timedelta(days=d)
                fecha_cierre_str = f'{fecha_cierre.year}-{fecha_cierre.month:02d}-{fecha_cierre.day:02d}'
                for table in lista_tablas:
                    ID_tabla = f"{ID_proyecto}.datos_crudos.{table}"
                    GCS_URI = f"{ruta_base}/distribuidor_{distribuidor}/{table}/{fecha_cierre_str}.csv"
                    cargar_datos_de_gcs_a_bigquery(GCS_URI, ID_tabla, locals()[f"esquema_tabla_{table}"])
            except BadRequest as err:
                print(err)
            except NotFound as err:
                print(err)