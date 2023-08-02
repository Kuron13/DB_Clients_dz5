import psycopg2

def create_db(conn, cur):
    cur.execute('''
            DROP TABLE IF EXISTS Phone;
            DROP TABLE IF EXISTS Client;
        ''')
    cur.execute('''
        CREATE TABLE IF NOT EXISTS Client(
            id SERIAL PRIMARY KEY NOT NULL,
            first_name VARCHAR(40) NOT NULL,
            last_name VARCHAR(40) NOT NULL,
            email VARCHAR(60) UNIQUE NOT NULL
        );
    ''')
    cur.execute('''
        CREATE TABLE IF NOT EXISTS Phone(
            id SERIAL PRIMARY KEY NOT NULL,
            client_id INTEGER NOT NULL REFERENCES Client(id),
            number BIGINT UNIQUE
        );
    ''')
    conn.commit()

def add_client(cur, first_name, last_name, email, number=None):
    cur.execute('''
        INSERT INTO Client(first_name, last_name, email)
        VALUES (%s, %s, %s)
        RETURNING id;
    ''', (first_name, last_name, email))
    client_id = list(cur.fetchall())[0]
    if number is not None:
        cur.execute('''
            INSERT INTO Phone(client_id, number)
            VALUES (%s, %s)
            RETURNING id, client_id, number;
        ''', (client_id, number))
    cur.execute('''
        SELECT c.id, c.first_name, c.last_name, c.email, p.number FROM Client c
        FULL JOIN Phone p ON c.id = p.client_id
        WHERE c.id = %s
        GROUP BY c.id, p.number
    ''', (client_id,))
    print('Добавлен клиент: ', cur.fetchall())

def add_phone(cur, client_id, number):
    cur.execute('''
        SELECT * FROM Client WHERE id = %s;
    ''', (client_id,))
    if cur.fetchall() is None:
        print(f'Клиента {client_id} нет в базе данных')
        return
    if cur.fetchone() is not None:
        print(f'Этот номер телефона уже прикреплён к клиенту №{client_id}')
        return
    else:
        cur.execute('''
            INSERT INTO Phone(client_id, number) VALUES (%s, %s)
        ''', (client_id, number))
        cur.execute('''
            SELECT c.id, c.first_name, c.last_name, c.email, p.number FROM Client c
            FULL JOIN Phone p ON c.id = p.client_id
            WHERE c.id = %s
            GROUP BY c.id, p.number
        ''', (client_id,))
        print('Добавлен номер телефона:', cur.fetchall())

def change_client(cur, client_id, first_name=None, last_name=None, email=None, old_number=None, new_number=None):
    if first_name is not None:
        cur.execute('''
            UPDATE Client SET first_name=%s WHERE id=%s;
        ''', (first_name, client_id))
    if last_name is not None:
        cur.execute('''
            UPDATE Client SET last_name=%s WHERE id=%s;
        ''', (last_name, client_id))
    if email is not None:
        cur.execute('''
            UPDATE Client SET email=%s WHERE id=%s;
        ''', (email, client_id))
    if old_number is not None and new_number is not None:
        cur.execute('''
            SELECT * FROM Phone WHERE client_id = %s AND number = %s
        ''', (client_id, old_number))
        if cur.fetchall() is None:
            print(f'Телефон {old_number} не был привязан к клиенту {client_id}')
            return
        else:
            cur.execute('''
                UPDATE Phone SET number = %s WHERE client_id = %s AND number = %s;
            ''', (new_number, client_id, old_number))
        print('У клиента изменён номер телефона:', cur.fetchall())
    cur.execute('''
        SELECT c.id, c.first_name, c.last_name, c.email, p.number FROM Client c
        FULL JOIN Phone p ON c.id = p.client_id
        WHERE c.id = %s
        GROUP BY c.id, p.number
    ''', (client_id,))
    print('Обновлена информация о клиенте: ', cur.fetchall())

def delete_phone(cur, client_id, number):
    cur.execute('''
        DELETE FROM Phone WHERE client_id = %s AND number = %s;
    ''', (client_id, number))
    cur.execute('''
        SELECT c.id, c.first_name, c.last_name, c.email, p.number FROM Client c
        FULL JOIN Phone p ON c.id = p.client_id
        WHERE c.id = %s
        GROUP BY c.id, p.number
    ''', (client_id,))
    print('Телефон удалён: ', cur.fetchall())

def delete_client(cur, client_id):
    cur.execute('''
        DELETE FROM Client WHERE id = %s;
    ''', (client_id,))
    cur.execute('''
        SELECT c.id, c.first_name, c.last_name, c.email, p.number FROM Client c
        FULL JOIN Phone p ON c.id = p.client_id
        WHERE c.id = %s
        GROUP BY c.id, p.number
    ''', (client_id,))
    print(f'Клиент №{client_id} удалён: {cur.fetchall()}')

def find_client(cur, first_name=None, last_name=None, email=None, number=None):
    params = [{'name': 'first_name', 'val': first_name}, {'name': 'last_name', 'val': last_name}, {'name': 'email', 'val': email}, {'name': 'number', 'val': number}]
    reqs_n = []
    reqs_v = []
    for par in params:
        if par['val'] is not None:
            reqs_n.append(f'{par["name"]}=%s')
            reqs_v.append(par['val'])
    sel = ''
    for req in reqs_n:
        if reqs_n.index(req) == 0:
            sel += req
        else:
            sel += (' AND ' + req)
    sel += ';'
    sel_full = '''SELECT c.id, c.first_name, c.last_name, c.email, p.number FROM Client c
        JOIN Phone p ON c.id = p.client_id
        WHERE '''
    sel_full = sel_full + sel
    cur.execute(sel_full, reqs_v[0:3])
    print('Найдено совпадение: ', cur.fetchall())

if __name__ == '__main__':
    conn = psycopg2.connect(database='Clients_DB', user='postgres', password='postgres')
    with conn.cursor() as cur:
        create_db(conn, cur)
        add_client(cur, 'Алексей', 'Попович', 'alyosha@gmail.com', 89001001100)
        add_client(cur, 'Добрыня', 'Никитич', 'dobriy@yandex.ru')
        add_client(cur, 'Змей', 'Горыныч', 'gorinich@mail.ru')
        add_client(cur, 'Илья', 'Муромец', 'muromec@yandex.ru', 89003003300)
        add_phone(cur, 1, 89001001122)
        change_client(cur, 2, first_name='ДОБРЫНЯ', new_number=89002002200)
        delete_phone(cur, 1, 89001001100)
        delete_client(cur, 3)
        find_client(cur, first_name='Илья', number=89003003300)
        pass
conn.close()
