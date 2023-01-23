import psycopg2

def create_table():
    cur.execute("""
        DROP TABLE phone;
        DROP TABLE client;
        """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS client(
        client_id SERIAL PRIMARY KEY,
        first_name VARCHAR(40) NOT NULL,
        last_name VARCHAR(40) NOT NULL,
        email VARCHAR(40) UNIQUE NOT NULL
    );
    """)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS phone(
        phone_id SERIAL PRIMARY KEY,
        number_phone INTEGER UNIQUE,
        client_id INTEGER NOT NULL REFERENCES client(client_id)
    );
    """)
    conn.commit()

def add_client (cur, first_name: str, last_name:str, email: str):
    cur.execute("""
    INSERT INTO client(first_name, last_name, email)
    VALUES (%s, %s, %s);
    """, (first_name, last_name, email))
    return conn.commit()

def add_phone(cur, number_phone: int, client_id: int):
    cur.execute("""
    INSERT INTO phone(number_phone, client_id)
    VALUES (%s, %s);
    """, (number_phone, client_id))
    return conn.commit()

def change_client(cur, client_id: int, column: str, new_data, replaceable_phone=None):
    if column == 'last_name':
        cur.execute(
        f"UPDATE client SET {column} = %s \
        WHERE client_id = %s;", (new_data, client_id))
    elif column == 'email':
        cur.execute(
        f"UPDATE client SET {column} = %s \
        WHERE client_id = %s;", (new_data, client_id))
    elif column == 'number_phone':
        cur.execute(
        f"UPDATE phone SET {column} = %s \
        WHERE client_id = (SELECT client_id FROM client WHERE client_id = %s) \
                            AND number_phone = %s;", (new_data, client_id, replaceable_phone))
    return conn.commit()

def delete_phone(cur, client_id, phone=None):
    cur.execute("""
    DELETE FROM phone
    WHERE client_id = %s OR number_phone = %s;
    """, (client_id, phone))
    return conn.commit()

def delete_client(cur, client_id):
    cur.execute("""
    DELETE FROM phone WHERE client_id = %s;
    """, (client_id, ))
    cur.execute("""
    DELETE FROM client WHERE client_id = %s
    """, (client_id,))
    return conn.commit()

def find_client(cur, column: str, data):
    cur.execute(
        f"SELECT * FROM client \
        FULL JOIN phone USING(client_id) \
        WHERE {column} = %s", (data,))
    return print(cur.fetchall())

if __name__ == '__main__':
    with psycopg2.connect(database='hw-bd5', user='postgres', password= '...') as conn:
        with conn.cursor() as cur:
            
            create_table()
            add_client(cur, 'Lenka', 'Sokolova', 'lena@mail.ru')
            add_client(cur, 'Petya', 'Ogurtsov', 'ogurets@mail.ru')
            add_client(cur, 'Petya', 'Second', 'dub@mail.ru')
            add_phone(cur, 505412, 1)
            add_phone(cur, 787835438, 1)
            add_phone(cur, 895648, 2)
            change_client(cur, 2, 'last_name', 'Ogurets')
            change_client(cur, 1, 'email', 'lenka@mail.ru')
            change_client(cur, 1, 'number_phone', 506123, 787835438)           
            find_client(cur, 'first_name', 'Petya')
            find_client(cur, 'number_phone', 506123)
            delete_phone(cur, 1, 506123)
            delete_client(cur, 1)
    conn.close()
