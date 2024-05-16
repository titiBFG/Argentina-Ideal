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
    
    #Primero hago la query para juntar la tabla de ventas con la ubicación
    generar_fact_ventas_temp = f"""
    SELECT
        tabla_cliente.ciudad,
        tabla_cliente.provincia,
        tabla_venta.codigo_sucursal,
        tabla_venta.codigo_cliente,
        tabla_venta.fecha_cierre_comercial,
        tabla_venta.SKU_codigo,
        tabla_venta.venta_unidades,
        tabla_venta.venta_importe
    FROM `{ID_proyecto}.datos_crudos.venta` AS tabla_venta
    JOIN `{ID_proyecto}.datos_crudos.cliente` AS tabla_cliente
        ON tabla_venta.codigo_sucursal = tabla_cliente.codigo_sucursal AND tabla_venta.codigo_cliente = tabla_cliente.codigo_cliente
    """

    generar_fact_ventas= f"""
    SELECT
        dim_cliente.id_cliente,
        dim_fechas.id_fecha,
        dim_producto.id_producto,
        dim_ubicacion.id_ubicacion,
        fact_ventas_temp.venta_unidades,
        fact_ventas_temp.venta_importe
    FROM `{ID_proyecto}.{datawarehouse_nombre}.fact_ventas_temp` AS fact_ventas_temp
    JOIN `{ID_proyecto}.{datawarehouse_nombre}.dim_ubicacion` AS dim_ubicacion
        ON fact_ventas_temp.ciudad = dim_ubicacion.Ciudad AND fact_ventas_temp.provincia = dim_ubicacion.Provincia
    JOIN `{ID_proyecto}.{datawarehouse_nombre}.dim_producto` AS dim_producto
        ON fact_ventas_temp.SKU_codigo = dim_producto.Codigo_SKU 
    JOIN `{ID_proyecto}.{datawarehouse_nombre}.dim_fechas` AS dim_fechas
        ON fact_ventas_temp.fecha_cierre_comercial = dim_fechas.fecha_cierre_comercial
    JOIN `{ID_proyecto}.{datawarehouse_nombre}.dim_cliente` AS dim_cliente
        ON fact_ventas_temp.codigo_sucursal = dim_cliente.codigo_sucursal AND fact_ventas_temp.codigo_cliente = dim_cliente.Codigo_cliente
    """

    #Creo la tabla temporal de la cual se apoyará la tabla de hechos de ventas
    ID_tabla_temporal = f"{ID_proyecto}.{datawarehouse_nombre}.fact_ventas_temp"
    crear_tabla(
            client=client,
            ID_tabla_a_crear=ID_tabla_temporal,
            query=generar_fact_ventas_temp,
            metodo_escritura="WRITE_TRUNCATE"
        )
    
    #Ahora si creo la tabla de hechos de ventas usando la tabla temporal
    ID_tabla_hechos = f"{ID_proyecto}.{datawarehouse_nombre}.fact_ventas"
    crear_tabla(
            client=client,
            ID_tabla_a_crear=ID_tabla_hechos,
            query=generar_fact_ventas,
            metodo_escritura="WRITE_TRUNCATE"
        )

    #Elimino la tabla temporal
    client.delete_table(ID_tabla_temporal, not_found_ok=True)
    print(f"Tabla temporal {ID_tabla_temporal} eliminada.")