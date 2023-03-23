import pandas as pd
import mysql.connector
import numpy as np;
from flask import Flask, jsonify
app = Flask(__name__)
mydb = mysql.connector.connect(host='localhost',user='root',password='12345',db='proyecto1db')
mycursor = mydb.cursor()
@app.route('/', methods=["GET"])
def Inicio():
    return "Se inicio flask correctamente"

@app.route('/cargar', methods=["GET"])
def Carga():
#--------------Separate excel sheets-------------------------------
    sheet_client = pd.read_excel("DB_Excel.xlsx", sheet_name='Cliente')
    sheet_category = pd.read_excel("DB_Excel.xlsx", sheet_name='Categoría')
    sheet_order = pd.read_excel("DB_Excel.xlsx", sheet_name='Orden')
    sheet_country = pd.read_excel("DB_Excel.xlsx", sheet_name='País')
    sheet_product = pd.read_excel("DB_Excel.xlsx", sheet_name='Producto')
    sheet_seller = pd.read_excel("DB_Excel.xlsx", sheet_name='Vendedor')
     #-------------------Insert data table country----------------------------
    sql_command = "INSERT INTO pais(id_pais, nombre) VALUES (%s, %s)"
    id_pais = np.asarray(sheet_country["id_pais"]).reshape(-1, 1);
    nombre_pais = np.asarray(sheet_country["nombre"]).reshape(-1, 1);
    for i,j in zip(id_pais, nombre_pais):
        for k,p in zip(i,j):
            val = (str(k), str(p))
            mycursor.execute(sql_command, val)
    mydb.commit() 
    #-------------------Insert data table category----------------------------
    sql_command = "INSERT INTO categoría(id_categoria, nombre) VALUES (%s, %s)"
    nombre_ca = np.asarray(sheet_category["nombre"]).reshape(-1, 1);
    id_caregoria = np.asarray(sheet_category["id_categoria"]).reshape(-1, 1);
    for i,j in zip(nombre_ca, id_caregoria):
        for k,p in zip(i,j):
            val = (str(p), str(k))
            mycursor.execute(sql_command, val)
    mydb.commit()
    #-------------------Insert data table client----------------------------
    sql_command = "INSERT INTO cliente(id_cliente, nombre, apellido, direccion, telefono, tarjeta, edad, salario, genero, id_pais) VALUES (%s, %s,%s,%s,%s,%s,%s,%s,%s,%s)"
    id_cliente = np.asarray(sheet_client["id_cliente"]).reshape(-1, 1);
    nombre_cliente = np.asarray(sheet_client["Nombre"]).reshape(-1, 1);
    apellido_cliente = np.asarray(sheet_client["Apellido"]).reshape(-1, 1);
    direccion_cliente = np.asarray(sheet_client["Direccion"]).reshape(-1, 1);
    telefono_cliente = np.asarray(sheet_client["Telefono"]).reshape(-1, 1);
    tarjeta_cliente = np.asarray(sheet_client["Tarjeta"]).reshape(-1, 1);
    edad_cliente = np.asarray(sheet_client["Edad"]).reshape(-1, 1);
    salario_cliente = np.asarray(sheet_client["Salario"]).reshape(-1, 1);
    genero_cliente = np.asarray(sheet_client["Genero"]).reshape(-1, 1);
    idpais_cliente = np.asarray(sheet_client["id_pais"]).reshape(-1, 1);
    for a,b,c,d,e,f,g,h,i,j in zip(id_cliente, nombre_cliente, apellido_cliente, direccion_cliente, telefono_cliente, tarjeta_cliente,edad_cliente, salario_cliente, genero_cliente, idpais_cliente):
        for a2,b2,c2,d2,e2,f2,g2,h2,i2,j2 in zip(a,b,c,d,e,f,g,h,i,j):
            val = (str(a2),str(b2),str(c2),str(d2),str(e2),str(f2),str(g2),str(h2),str(i2),str(j2))
            mycursor.execute(sql_command, val)
    mydb.commit()
    #-------------------Insert data table order----------------------------
    sql_command = "INSERT INTO orden(indice,id_orden, linea_orden, fecha_orden, id_cliente, id_vendedor, id_producto, cantidad) VALUES (%s,%s, %s,%s,%s,%s,%s,%s)"
    index = 1
    id_orden = np.asarray(sheet_order["id_orden"]).reshape(-1, 1);
    linea_orden = np.asarray(sheet_order["linea_orden"]).reshape(-1, 1);
    fecha_orden = np.asarray(sheet_order["fecha_orden"]).reshape(-1, 1);
    id_cliente_orden = np.asarray(sheet_order["id_cliente"]).reshape(-1, 1);
    id_vendedor_orden = np.asarray(sheet_order["id_vendedor"]).reshape(-1, 1);
    id_producto_orden = np.asarray(sheet_order["id_producto"]).reshape(-1, 1);
    cantidad = np.asarray(sheet_order["cantidad"]).reshape(-1, 1);
    for a,b,c,d,e,f,g in zip(id_orden, linea_orden, fecha_orden, id_cliente_orden, id_vendedor_orden, id_producto_orden,cantidad):
        for a2,b2,c2,d2,e2,f2,g2 in zip(a,b,c,d,e,f,g):
            val = (str(index),str(a2),str(b2),str(c2),str(d2),str(e2),str(f2),str(g2))
            mycursor.execute(sql_command, val)
            index +=1
    mydb.commit()

    #-------------------Insert data table product----------------------------
    sql_command = "INSERT INTO producto(id_producto, nombre, precio, id_categoria) VALUES (%s,%s,%s,%s)"
    id_producto = np.asarray(sheet_product["id_producto"]).reshape(-1, 1);
    nombre_producto = np.asarray(sheet_product["Nombre"]).reshape(-1, 1);
    precio_producto = np.asarray(sheet_product["Precio"]).reshape(-1, 1);
    id_categoria_producto = np.asarray(sheet_product["id_categoria"]).reshape(-1, 1);
    for a,b,c,d in zip(id_producto, nombre_producto,precio_producto,id_categoria_producto):
        for a2,b2,c2,d2 in zip(a,b,c,d):
            val = (str(a2), str(b2), str(c2), str(d2))
            mycursor.execute(sql_command, val)
    mydb.commit()
    #-------------------Insert data table seller----------------------------
    sql_command = "INSERT INTO vendedor(id_vendedor, nombre,id_pais) VALUES (%s,%s,%s)"
    id_vendedor = np.asarray(sheet_seller["id_vendedor"]).reshape(-1, 1);
    nombre_vendedor = np.asarray(sheet_seller["nombre"]).reshape(-1, 1);
    id_pais_vendedor = np.asarray(sheet_seller["id_pais"]).reshape(-1, 1);
    for a,b,c in zip(id_vendedor,nombre_vendedor,id_pais_vendedor):
        for a2,b2,c2 in zip(a,b,c):
            val = (str(a2), str(b2), str(c2))
            mycursor.execute(sql_command, val)
    mydb.commit()
    return jsonify({ "Mensaje": "Carga exitosa :D" })



@app.route('/C1', methods=["GET"])
def Consulta_1():
    mycursor.execute('''SELECT id_cliente, apellido,nombre,nombre_pais, SUM(cantidad) as cantidad, SUM(cantidad * precio) as monto
                        FROM orden
                        INNER JOIN cliente USING(id_cliente)
                        INNER JOIN pais USING(id_pais)
                        INNER JOIN producto USING(id_producto)
                        GROUP BY id_cliente, nombre
                        ORDER BY cantidad DESC
                        LIMIT 1''')
    Executed_DATA = mycursor.fetchall()
    Respuesta = {}
    Respuesta['Consulta_1'] = []
    for i in Executed_DATA:
            Respuesta['Consulta_1'].append({
            'Id_Cliente': i[0],
            'Nombre del Cliente': i[2],
            'Apellido del Cliente': i[1],
            'Pais del Cliente': i[3],
            'Monto': i[5]})
    return jsonify({ "Respuesta": Respuesta })


@app.route('/C2', methods=["GET"])
def Consulta_2():
    mycursor.execute('''
                        (
                        SELECT id_producto, nombre_p, id_categoria,nombre_c, SUM(cantidad) as cantidad, SUM(cantidad * precio) as monto
                        FROM orden
                        INNER JOIN producto USING(id_producto)
                        INNER JOIN categoría USING(id_categoria)
                        GROUP BY id_producto, nombre_p,nombre_c
                        ORDER BY cantidad DESC
                        LIMIT 1
                        )UNION ALL(
                        SELECT id_producto, nombre_p, id_categoria,nombre_c, SUM(cantidad) as cantidad, SUM(cantidad * precio) as monto
                        FROM orden
                        INNER JOIN producto USING(id_producto)
                        INNER JOIN categoría USING(id_categoria)
                        GROUP BY id_producto, nombre_p,nombre_c
                        ORDER BY cantidad ASC
                        LIMIT 5
                        )''')
    Respuesta = {}
    Respuesta['Consulta_2'] = []
    Executed_DATA = mycursor.fetchall()
    for i in Executed_DATA:
            Respuesta['Consulta_2'].append({
            'Id_producto': i[0],
            'Nombre del Producto': i[1],
            'Categoria': i[3],
            'Cantidad': i[4],
            'Monto': i[5]})
    return jsonify({ "Respuesta": Respuesta })


@app.route('/C3', methods=["GET"])
def Consulta_3():
    mycursor.execute('''SELECT id_vendedor,nombre,precio, SUM(cantidad) as cantidad, SUM(cantidad * precio) as monto
                        FROM orden
                        INNER JOIN vendedor USING(id_vendedor)
                        INNER JOIN producto USING(id_producto)
                        GROUP BY id_vendedor, nombre
                        ORDER BY cantidad DESC
                        LIMIT 1''')
    Executed_DATA = mycursor.fetchall()
    Respuesta = {}
    Respuesta['Consulta_3'] = []
    for i in Executed_DATA:
            Respuesta['Consulta_3'].append({
            'Id_Vendedor': i[0],
            'Nombre del Vendedor': i[1],
            'Monto': i[4]})
    return jsonify({ "Respuesta": Respuesta })


@app.route('/C4', methods=["GET"])
def Consulta_4():
    mycursor.execute('''(
                        SELECT id_pais,nombre_pais,SUM(cantidad * precio) as monto
                        FROM orden
                        INNER JOIN producto USING(id_producto)
                        INNER JOIN vendedor USING(id_vendedor)
                        INNER JOIN pais USING(id_pais)
                        GROUP BY id_pais
                        ORDER BY monto DESC
                        LIMIT 1
                        ) UNION ALL 
                        (
                        SELECT id_pais,nombre_pais,SUM(cantidad * precio) as monto
                        FROM orden
                        INNER JOIN producto USING(id_producto)
                        INNER JOIN vendedor USING(id_vendedor)
                        INNER JOIN pais USING(id_pais)
                        GROUP BY id_pais
                        ORDER BY monto ASC
                        LIMIT 1
                    )''')
    Executed_DATA = mycursor.fetchall()
    Respuesta = {}
    Respuesta['Consulta_4'] = []
    for i in Executed_DATA:
            Respuesta['Consulta_4'].append({
            'Pais': i[1],
            'Monto': i[2]})
    return jsonify({ "Respuesta": Respuesta })
    


@app.route('/C5', methods=["GET"])
def Consulta_5():
    mycursor.execute('''SELECT id_pais,nombre_pais,SUM(cantidad * precio) as monto
                        FROM orden
                        INNER JOIN producto USING(id_producto)
                        INNER JOIN vendedor USING(id_vendedor)
                        INNER JOIN pais USING(id_pais)
                        GROUP BY id_pais, nombre_pais
                        ORDER BY monto ASC
                        LIMIT 5''')
    Executed_DATA = mycursor.fetchall()
    Respuesta = {}
    Respuesta['Consulta_5'] = []
    for i in Executed_DATA:
            Respuesta['Consulta_5'].append({
            'Id_Pais': i[0],
            'Pais': i[1],
            'Monto': i[2]})
    return jsonify({ "Respuesta": Respuesta })



@app.route('/C6', methods=["GET"])
def Consulta_6():
    mycursor.execute('''(
                        SELECT id_categoria,nombre_c,SUM(cantidad) as cantidad, SUM(cantidad * precio) as monto
                        FROM orden
                        INNER JOIN producto USING(id_producto)
                        INNER JOIN categoría USING(id_categoria)
                        GROUP BY id_categoria,nombre_c
                        ORDER BY cantidad DESC
                        LIMIT 1
                        )
                        UNION ALL
                        (
                        SELECT id_categoria,nombre_c,SUM(cantidad) as cantidad,SUM(cantidad * precio) as monto
                        FROM orden
                        INNER JOIN producto USING(id_producto)
                        INNER JOIN categoría USING(id_categoria)
                        GROUP BY id_categoria,nombre_c
                        ORDER BY cantidad ASC
                        LIMIT 1
                        )''')
    Executed_DATA = mycursor.fetchall()
    Respuesta = {}
    Respuesta['Consulta_6'] = []
    for i in Executed_DATA:
            Respuesta['Consulta_6'].append({
            'Nombre': i[1],
            'Unidades': i[2],
            'Monto': i[3]})
    return jsonify({ "Respuesta": Respuesta })


@app.route('/C7', methods=["GET"])
def Consulta_7():
    mycursor.execute('''SELECT *, MAX(respuesta_cantidad) as respuesta_cantidad, monto
                    FROM (
                    SELECT  categoría.id_categoria, pais.id_pais, pais.nombre_pais ,categoría.nombre_c,SUM(orden.cantidad) as respuesta_cantidad, SUM(orden.cantidad * producto.precio) as monto
                    FROM orden
                    INNER JOIN producto USING(id_producto)
                    INNER JOIN categoría USING(id_categoria)
                    INNER JOIN cliente USING(id_cliente)
                    INNER JOIN pais USING(id_pais)
                    GROUP BY pais.id_pais, categoría.id_categoria
                    ORDER BY respuesta_cantidad DESC
                    ) as paises
                    GROUP BY paises.id_pais''')
    Executed_DATA = mycursor.fetchall()
    Respuesta = {}
    Respuesta['Consulta_7'] = []
    for i in Executed_DATA:
            Respuesta['Consulta_7'].append({
            'Nombre Pais': i[2],
            'Nombre Categoria': i[3],
            'Cantidad de unidades': i[4],
            'Monto': i[5]})
    return jsonify({ "Respuesta": Respuesta })



@app.route('/C8', methods=["GET"])
def Consulta_8():
    mycursor.execute('''SELECT fecha_orden,nombre_pais, MONTH(fecha_orden) AS mes_orden, SUM(cantidad * precio) AS monto
                        FROM orden
                        INNER JOIN producto USING(id_producto)
                        INNER JOIN vendedor USING(id_vendedor)
                        INNER JOIN pais USING(id_pais)
                        WHERE id_pais = 10
                        GROUP BY mes_orden 
                        ORDER BY mes_orden ASC''')
    Executed_DATA = mycursor.fetchall()
    Respuesta = {}
    Respuesta['Consulta_8'] = []
    for i in Executed_DATA:
            Respuesta['Consulta_8'].append({
            'Mes': i[2],
            'Monto': i[3]})
    return jsonify({ "Respuesta": Respuesta })


@app.route('/C9', methods=["GET"])
def Consulta_9():
    mycursor.execute('''(
                        SELECT fecha_orden, MONTH(fecha_orden) AS mes_orden, SUM(cantidad * precio) AS monto
                        FROM orden
                        INNER JOIN producto USING(id_producto)
                        INNER JOIN vendedor USING(id_vendedor)
                        GROUP BY mes_orden 
                        ORDER BY mes_orden ASC
                        LIMIT 1
                        )
                        UNION ALL
                        (
                        SELECT fecha_orden, MONTH(fecha_orden) AS mes_orden, SUM(cantidad * precio) AS monto
                        FROM orden
                        INNER JOIN producto USING(id_producto)
                        INNER JOIN vendedor USING(id_vendedor)
                        GROUP BY mes_orden 
                        ORDER BY mes_orden DESC
                        LIMIT 1
                        )''')
    Executed_DATA = mycursor.fetchall()
    Respuesta = {}
    Respuesta['Consulta_9'] = []
    for i in Executed_DATA:
            Respuesta['Consulta_9'].append({
            'Mes': i[1],
            'Monto': i[2]})
    return jsonify({ "Respuesta": Respuesta })



@app.route('/C10', methods=["GET"])
def Consulta_10():
    mycursor.execute('''SELECT id_categoria, id_producto,nombre_c, nombre_p,SUM(cantidad * precio) as monto
                        FROM orden
                        INNER JOIN producto USING(id_producto)
                        INNER JOIN categoría USING(id_categoria)
                        WHERE id_categoria = 15
                        GROUP BY id_categoria, nombre_c,nombre_p,id_producto
                        ORDER BY monto ASC''')
    Executed_DATA = mycursor.fetchall()
    Respuesta = {}
    Respuesta['Consulta_10'] = []
    index = 1
    for i in Executed_DATA:
            Respuesta['Consulta_10'].append({
            'Indice': index,
            'Id_Producto': i[1],
            'Nomre': i[3],
            'Monto': i[4],})
            index += 1
    return jsonify({ "Respuesta": Respuesta })


if __name__ == '__main__':
    app.run(debug=True, port=3000)
