import json
from urllib.parse import parse_qs
from urllib.request import Request, urlopen
from urllib.error import HTTPError
from urllib.parse import urlencode
import psycopg2
from decimal import Decimal

def lambda_handler(event, context):
    creation_db_script = """
        ```sql
            CREATE DATABASE nombre_de_tu_base_de_datos;
    
            \c nombre_de_tu_base_de_datos;
    
            CREATE TABLE product_category (
                id INTEGER PRIMARY KEY,
                name VARCHAR(100),
                parent_id INTEGER, 
                FOREIGN KEY (parent_id) REFERENCES product_category(id)
            );
    
            CREATE TABLE product_template (
                id INTEGER PRIMARY KEY,
                name VARCHAR(100),
                type VARCHAR(100),
                categ_id INTEGER,
                list_price NUMERIC,
                FOREIGN KEY (categ_id) REFERENCES product_category(id)
            );
    
            CREATE TABLE product_attribute (
                id INTEGER PRIMARY KEY,
                name VARCHAR(100)
            );
    
            CREATE TABLE product_attribute_value (
                id INTEGER PRIMARY KEY,
                name VARCHAR(100),
                attribute_id INTEGER,
                FOREIGN KEY (attribute_id) REFERENCES product_attribute(id)
            );
    
            CREATE TABLE product_template_attribute_value (
                id INTEGER PRIMARY KEY,
                product_tmpl_id INTEGER,
                product_attribute_value_id,
                price_extra NUMERIC,
                FOREIGN KEY (product_tmpl_id) REFERENCES product_template(id),
                FOREIGN KEY (product_attribute_value_id) REFERENCES product_attribute_value(id)
            );
    
            CREATE TABLE product_product (
                id INTEGER PRIMARY KEY,
                product_tmpl_id INTEGER,
                FOREIGN KEY (product_tmpl_id) REFERENCES product_template(id)
            );
    
            CREATE TABLE res_partner (
                id INTEGER PRIMARY KEY,
                name VARCHAR(100),
                email VARCHAR(100)
            );
    
            CREATE TABLE sale_order (
                id INTEGER PRIMARY KEY,
                name VARCHAR(100),
                date_order timestamp without time zone,
                partner_id INTEGER,
                amount_untaxed NUMERIC,
                amount_total NUMERIC,
                FOREIGN KEY (partner_id) REFERENCES res_partner(id)
            );
    
            CREATE TABLE sale_order_line (
                id INTEGER PRIMARY KEY,
                order_id INTEGER,
                name VARCHAR(100),
                price_unit NUMERIC,
                price_subtotal NUMERIC,
                price_total NUMERIC,
                product_id INTEGER, 
                product_uom_qty NUMERIC,
                FOREIGN KEY (order_id) REFERENCES sale_order(id),
                FOREIGN KEY (product_id) REFERENCES product_product(id)
            );
        ```
    """
    api_key = ""
    text_for_query = json.loads(event["body"]).get("prompt", None)
    prompt="Eres un asistente SQL de un módulo del ERP Odoo. Tú misión es convertir una consulta en lenguaje natural a una query de sql que devuelva los datos que te piden. Solo debes contestar con la consulta SQL, nada más. Conviérteme a una query sql la siguiente petición:" + text_for_query + "\n, Dentro de una base de datos postgres creada con el siguiente script: " + creation_db_script + "\n Quiero que utilices la sintaxis 'table_name.attribute' para las columnas del select.",
    query = chat_gpt_api(prompt, api_key).get("choices", [])[0].get("text", "")
    conn = psycopg2.connect(
        host='odoo.c4i4wjtcmhvu.eu-west-3.rds.amazonaws.com',
        user='odoo',
        password='2ZhlBS9ihlX6mkna5ui6',
        dbname='odoo'
    )
    cur = conn.cursor()
    cur.execute(query)
    rows = cur.fetchall()
    column_names = [desc[0] for desc in cur.description]
    
    return {
        'statusCode': 200,
        "isBase64Encoded": False,
        "headers": { "Access-Control-Allow-Origin" : "*",
                    "Access-Control-Allow-Credentials" : True,
                    "Content-Type": "application/json" },
        'body': json.dumps({
            'query': query,
            'res': generate_result_from_query(rows, column_names)
        })
    }
    
def generate_result_from_query(rows, column_names):
    result_dict = {}
    for column_name in column_names:
        result_dict[column_name] = []
    
    for row in rows:
        for i, column_name in enumerate(column_names):
            value = row[i]
            if isinstance(value, Decimal):
                value = float(value)
            result_dict[column_name].append(value)
    
    return result_dict
    
def chat_gpt_api(prompt, api_key, model='text-davinci-003', max_tokens=100, n=1, temperature=0.7):
    url = 'https://api.openai.com/v1/engines/{}/completions'.format(model)
    headers = {
        'Content-Type': 'application/json',
        'Authorization': 'Bearer {}'.format(api_key)
    }
    
    data = {
        'prompt': prompt,
        'n': n,
        'max_tokens': max_tokens,
        'temperature': temperature
    }
    
    try:
        req = Request(url, headers=headers, data=json.dumps(data).encode('utf-8'))
        response = urlopen(req)
        result = json.loads(response.read().decode('utf-8'))
        return result
    except HTTPError as e:
        print('Error {}: {}'.format(e.code, e.reason))
        return None
