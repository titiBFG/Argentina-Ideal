from google.cloud import bigquery

def crear_tabla(client, ID_tabla_a_crear, query, metodo_escritura):
    """
    Esta función crea una nueva tabla en Google BigQuery utilizando una consulta SQL y la disposición de escritura dada.

    Parameters:
    client (bigquery.Client): El cliente de BigQuery.
    ID_tabla_a_crear (str): El ID de la tabla que se va a crear en el formato 'proyecto.dataset.tabla'.
    query (str): La consulta SQL que se va a ejecutar para llenar la tabla.
    metodo_escritura (str): La disposición de escritura para la tabla. Puede ser uno de los siguientes:
        - 'WRITE_TRUNCATE': Si la tabla existe sobre escribe los datos.
        - 'WRITE_APPEND': Si la tabla existe agrega datos en la tabla.
        - 'WRITE_EMPTY': Solo escribe datos cuando la tabla existe y no tiene datos.

    Returns:
    None. Si la ejecución de la consulta es exitosa, imprime el resultado. Si se produce un error, imprime el mensaje de error.
    """

    # Crea un objeto QueryJobConfig con la disposición de escritura especificada
    job_config = bigquery.QueryJobConfig(
        destination = ID_tabla_a_crear,
        write_disposition = metodo_escritura
    )

    # Ejecuta la consulta de manera asíncrona
    query_job = client.query(query, job_config=job_config)

    # Espera a que finalice el trabajo de consulta y maneja cualquier excepción
    try:
        query_job.result()
        print(f"Tabla {ID_tabla_a_crear} creada y datos insertados con éxito.")
    except Exception as err:
        print(f"Error al crear la tabla {ID_tabla_a_crear}: {err}")

if __name__ == '__main__':
    ID_proyecto = "usm-infra-grupo8-401213"
    datawarehouse_nombre = "datawarehouse_argideal"
    client = bigquery.Client(project=ID_proyecto)

    #Queries para crear tablas temporales
    generar_dim_fechas_temp = f"""
        SELECT DISTINCT
            stock.fecha_cierre_comercial AS fecha_cierre_comercial
        FROM `{ID_proyecto}.datos_crudos.stock` as stock;
    """

    generar_dim_producto_temp = f"""
        SELECT DISTINCT
            stock.SKU_codigo AS sku_codigo,
            stock.SKU_descripcion AS descripcion
        FROM `{ID_proyecto}.datos_crudos.stock` as stock;
    """

    generar_dim_ubicacion_temp = f"""
        SELECT DISTINCT
            cliente.provincia AS Provincia,
            cliente.ciudad AS Ciudad
        FROM `{ID_proyecto}.datos_crudos.cliente` as cliente;
    """

    generar_dim_cliente_temp = f"""
        SELECT DISTINCT
            cliente.codigo_sucursal,
            cliente.codigo_cliente AS Codigo_cliente,
            cliente.n_distribuidor,
            cliente.nombre_cliente,
            cliente.direccion,
            cliente.razon_social,
            cliente.cuit,
            cliente.estado,
            cliente.telefono,
            cliente.fecha_alta,
            cliente.fecha_baja,
            cliente.tipo_negocio
        FROM `{ID_proyecto}.datos_crudos.cliente` as cliente;
    """

    #Queries para crear tablas finales con IDs incrementales
    generar_dim_fechas = f"""
        SELECT
            ROW_NUMBER() OVER() AS id_fecha,
            fecha_cierre_comercial,
            EXTRACT(DAY FROM fecha_cierre_comercial) AS dia,
            EXTRACT(MONTH FROM fecha_cierre_comercial) AS mes,
            EXTRACT(QUARTER FROM fecha_cierre_comercial) AS trimestre
        FROM `{ID_proyecto}.{datawarehouse_nombre}.temp_fechas`
        ORDER BY id_fecha;
    """

    generar_dim_producto = f"""
        SELECT
            ROW_NUMBER() OVER() AS id_producto,
            sku_codigo AS Codigo_SKU,
            descripcion AS nombre_producto
        FROM `{ID_proyecto}.{datawarehouse_nombre}.temp_producto`
        ORDER BY id_producto;
    """

    generar_dim_ubicacion = f"""
        SELECT
            ROW_NUMBER() OVER() AS id_ubicacion,
            Provincia,
            Ciudad
        FROM `{ID_proyecto}.{datawarehouse_nombre}.temp_ubicacion`
        ORDER BY id_ubicacion;
    """

    generar_dim_cliente = f"""
        SELECT
            ROW_NUMBER() OVER() AS id_cliente,
            codigo_sucursal,
            Codigo_cliente,
            n_distribuidor,
            nombre_cliente,
            direccion,
            razon_social,
            cuit,
            estado,
            telefono,
            fecha_alta,
            fecha_baja,
            tipo_negocio
        FROM `{ID_proyecto}.{datawarehouse_nombre}.temp_cliente`
        ORDER BY id_cliente;
    """

    #Genero listas para asociar las tablas con las queriesque las generan
    lista_tablas_temp = ['temp_fechas', 'temp_producto', 'temp_ubicacion', 'temp_cliente']
    lista_querys_temp = [generar_dim_fechas_temp, generar_dim_producto_temp, generar_dim_ubicacion_temp, generar_dim_cliente_temp]

    lista_tablas_final = ['dim_fechas', 'dim_producto', 'dim_ubicacion', 'dim_cliente']
    lista_querys_final = [generar_dim_fechas, generar_dim_producto, generar_dim_ubicacion, generar_dim_cliente]

    #Primero genero las tablas temporales
    for tabla, query in zip(lista_tablas_temp, lista_querys_temp):
        TABLE_ID = f"{ID_proyecto}.{datawarehouse_nombre}.{tabla}"
        crear_tabla(
            client=client,
            ID_tabla_a_crear=TABLE_ID,
            query=query,
            metodo_escritura="WRITE_TRUNCATE"
        )

    #Y luego genero las tablas finales, cuyos querys se apoyan en las tablas temporales
    for tabla, query in zip(lista_tablas_final, lista_querys_final):
        TABLE_ID = f"{ID_proyecto}.{datawarehouse_nombre}.{tabla}"
        
        crear_tabla(
            client=client,
            ID_tabla_a_crear=TABLE_ID,
            query=query,
            metodo_escritura="WRITE_TRUNCATE"
        )

        #Luego de crear la tabla, elimino la tabla temporal correspondiente
        TEMP_TABLE_ID = f"{ID_proyecto}.{datawarehouse_nombre}.temp_{tabla.split('_')[1]}"
        client.delete_table(TEMP_TABLE_ID, not_found_ok=True)
        print(f"Tabla temporal {TEMP_TABLE_ID} eliminada.")  
