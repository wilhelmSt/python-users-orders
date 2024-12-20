import psycopg2
import os
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import List


DB_CONFIG = {
    "dbname": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "host": os.getenv("DB_HOST"),
    "port": os.getenv("DB_PORT"),
}


app = FastAPI()


class Usuario(BaseModel):
    nome: str
    email: str

class Produto(BaseModel):
    nome: str
    preco: float

class PedidoProduto(BaseModel):
    pedido_id: int
    produto_id: int
    quantidade: int


def get_connection():
    return psycopg2.connect(**DB_CONFIG)


def create_tables():
    with get_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS usuarios (
                id SERIAL PRIMARY KEY,
                nome TEXT NOT NULL,
                email TEXT UNIQUE NOT NULL
            );

            CREATE TABLE IF NOT EXISTS produtos (
                id SERIAL PRIMARY KEY,
                nome TEXT NOT NULL,
                preco NUMERIC NOT NULL
            );

            CREATE TABLE IF NOT EXISTS pedidos (
                id SERIAL PRIMARY KEY,
                usuario_id INTEGER NOT NULL,
                data TIMESTAMP DEFAULT NOW(),
                FOREIGN KEY (usuario_id) REFERENCES usuarios (id)
            );

            CREATE TABLE IF NOT EXISTS pedido_produto (
                pedido_id INTEGER NOT NULL,
                produto_id INTEGER NOT NULL,
                quantidade INTEGER NOT NULL,
                PRIMARY KEY (pedido_id, produto_id),
                FOREIGN KEY (pedido_id) REFERENCES pedidos (id),
                FOREIGN KEY (produto_id) REFERENCES produtos (id)
            );
            """)
            conn.commit()


def populate_tables():
    with get_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute("""
            INSERT INTO usuarios (nome, email) VALUES 
                ('Alice', 'alice@example.com'),
                ('Bob', 'bob@example.com')
            ON CONFLICT DO NOTHING;

            INSERT INTO produtos (nome, preco) VALUES 
                ('Produto A', 10.50),
                ('Produto B', 25.00),
                ('Produto C', 7.25)
            ON CONFLICT DO NOTHING;

            INSERT INTO pedidos (usuario_id) VALUES (1), (2)
            ON CONFLICT DO NOTHING;

            INSERT INTO pedido_produto (pedido_id, produto_id, quantidade) VALUES
                (1, 1, 2),
                (1, 2, 1),
                (2, 3, 5)
            ON CONFLICT DO NOTHING;
            """)
            conn.commit()


@app.post("/usuarios", response_model=dict)
def create_usuario(usuario: Usuario):
    with get_connection() as conn:
        with conn.cursor() as cursor:
            try:
                cursor.execute("INSERT INTO usuarios (nome, email) VALUES (%s, %s) RETURNING id", 
                               (usuario.nome, usuario.email))
                user_id = cursor.fetchone()[0]
                conn.commit()
                return {"id": user_id}
            except psycopg2.IntegrityError:
                conn.rollback()
                raise HTTPException(status_code=400, detail="Email já cadastrado.")


@app.get("/usuarios", response_model=List[dict])
def get_usuarios():
    with get_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute("SELECT * FROM usuarios")
            rows = cursor.fetchall()
    return [{"id": row[0], "nome": row[1], "email": row[2]} for row in rows]


@app.put("/usuarios/{id}", response_model=dict)
def update_usuario(id: int, usuario: Usuario):
    with get_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute("UPDATE usuarios SET nome = %s, email = %s WHERE id = %s", 
                           (usuario.nome, usuario.email, id))
            conn.commit()
    return {"status": "updated"}


@app.delete("/usuarios/{id}", response_model=dict)
def delete_usuario(id: int):
    with get_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute("DELETE FROM usuarios WHERE id = %s", (id,))
            conn.commit()
    return {"status": "deleted"}


@app.get("/relatorios/pedidos_por_usuario", response_model=List[dict])
def pedidos_por_usuario():
    with get_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT u.nome, COUNT(p.id) AS total_pedidos
                FROM usuarios u
                LEFT JOIN pedidos p ON u.id = p.usuario_id
                GROUP BY u.id
                ORDER BY total_pedidos DESC;
            """)
            rows = cursor.fetchall()
    return [{"usuario": row[0], "total_pedidos": row[1]} for row in rows]


@app.get("/relatorios/total_gasto_por_pedido", response_model=List[dict])
def total_gasto_por_pedido():
    with get_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT p.id AS pedido_id, SUM(pp.quantidade * pr.preco) AS total_gasto
                FROM pedidos p
                INNER JOIN pedido_produto pp ON p.id = pp.pedido_id
                INNER JOIN produtos pr ON pp.produto_id = pr.id
                GROUP BY p.id
                ORDER BY total_gasto DESC;
            """)
            rows = cursor.fetchall()
    return [{"pedido_id": row[0], "total_gasto": row[1]} for row in rows]


create_tables()
populate_tables()