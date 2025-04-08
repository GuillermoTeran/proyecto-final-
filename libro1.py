# importar librerías
import pandas as pd
from sqlalchemy import create_engine

# configuración
db_config = {
    'user': 'practicum_student',
    'pwd': 's65BlTKV3faNIGhmvJVzOqhs',
    'host': 'rc1b-wcoijxj3yxfsf3fs.mdb.yandexcloud.net',
    'port': 6432,
    'db': 'data-analyst-final-project-db'
}

# conexión
connection_string = 'postgresql://{}:{}@{}:{}/{}'.format(
    db_config['user'],
    db_config['pwd'],
    db_config['host'],
    db_config['port'],
    db_config['db']
)

engine = create_engine(connection_string, connect_args={'sslmode': 'require'})

# consulta
query = '''
SELECT COUNT(*) AS num_libros_post_2000
FROM books
WHERE publication_date > '2000-01-01';
'''

# ejecutar consulta y mostrar resultado
df = pd.read_sql(query, con=engine)
print(df)

# Mostrar primeras filas de cada tabla
for table in ['books', 'authors', 'publishers', 'ratings', 'reviews']:
    print(f'\nTabla: {table}')
    print(pd.read_sql(f'SELECT * FROM {table} LIMIT 5', con=engine))

# Consultas SQL
# 1. Número de libros publicados después del 1 de enero de 2000
query1 = '''
SELECT COUNT(*) AS num_libros_post_2000
FROM books
WHERE publication_date > '2000-01-01';
'''
print("\n1. Número de libros publicados después del 1 de enero de 2000:")
print(pd.read_sql(query1, con=engine))

# 2. Número de reseñas y calificación promedio por libro
query2 = '''
SELECT
    b.title,
    COUNT(DISTINCT r.review_id) AS num_reviews,
    ROUND(AVG(rt.rating), 2) AS avg_rating
FROM books b
LEFT JOIN reviews r ON b.book_id = r.book_id
LEFT JOIN ratings rt ON b.book_id = rt.book_id
GROUP BY b.title
ORDER BY num_reviews DESC;
'''
print("\n2. Número de reseñas y calificación promedio por libro:")
print(pd.read_sql(query2, con=engine))

# 3. Editorial con más libros de más de 50 páginas
query3 = '''
SELECT p.publisher, COUNT(*) AS num_books
FROM books b
JOIN publishers p ON b.publisher_id = p.publisher_id
WHERE b.num_pages > 50
GROUP BY p.publisher
ORDER BY num_books DESC
LIMIT 1;
'''
print("\n3. Editorial con más libros de más de 50 páginas:")
print(pd.read_sql(query3, con=engine))

# 4. Autor con mejor calificación promedio (solo libros con ≥ 50 calificaciones)
query4 = '''
SELECT a.author, ROUND(AVG(rt.rating), 2) AS avg_rating
FROM books b
JOIN authors a ON b.author_id = a.author_id
JOIN ratings rt ON b.book_id = rt.book_id
GROUP BY a.author
HAVING COUNT(rt.rating_id) >= 50
ORDER BY avg_rating DESC
LIMIT 1;
'''
print("\n4. Autor con mejor calificación promedio (libros con al menos 50 calificaciones):")
print(pd.read_sql(query4, con=engine))

# 5. Promedio de reseñas de texto entre usuarios que calificaron más de 50 libros
query5 = '''
SELECT ROUND(AVG(review_count), 2) AS avg_reviews
FROM (
    SELECT rv.username, COUNT(DISTINCT rv.review_id) AS review_count
    FROM ratings rt
    JOIN reviews rv ON rt.username = rv.username
    GROUP BY rv.username
    HAVING COUNT(DISTINCT rt.rating_id) > 50
) AS subquery;
'''
print("\n5. Promedio de reseñas de texto entre usuarios que calificaron más de 50 libros:")
print(pd.read_sql(query5, con=engine))

# Conclusiones:
# 1. El número de libros publicados después del 1 de enero de 2000 es significativo, con un total de 819 libros. 
#    Esto indica que la mayoría de los libros en la base de datos son recientes, lo cual podría ser útil para identificar 
#    las tendencias actuales en la industria editorial.
#
# 2. La cantidad de reseñas y las calificaciones promedio por libro varían considerablemente. Algunos libros populares 
#    tienen muchas reseñas, mientras que otros tienen muy pocas o ninguna. Este comportamiento sugiere que algunos libros 
#    tienen un mayor interés por parte de los lectores que otros, lo cual podría ser importante para los editores y autores.
#
# 3. La editorial con más libros de más de 50 páginas es Penguin Books, con 42 libros. Esto podría indicar que esta 
#    editorial publica libros más largos o que tiene un enfoque en libros de mayor extensión.
#
# 4. Diana Gabaldon se destaca como el autor con la mejor calificación promedio, con una calificación de 4.3 para sus 
#    libros. Esto refleja una alta satisfacción de los lectores con sus obras, lo que sugiere que es un autor popular 
#    y apreciado.
#
# 5. Los usuarios que calificaron más de 50 libros suelen dejar un promedio de 24.33 reseñas de texto por persona. 
#    Esto sugiere que estos usuarios son muy activos en la plataforma, proporcionando no solo calificaciones, sino 
#    también comentarios detallados, lo que podría ser valioso para mejorar la calidad de las recomendaciones y reseñas.
#
# Recomendaciones:
# 1. Para los editores y autores, podría ser útil investigar más a fondo qué tipo de libros están siendo más populares 
#    en la base de datos (por ejemplo, libros post-2000) para adaptar sus publicaciones a las preferencias actuales del 
#    mercado.
# 
# 2. Se sugiere promover más la participación de los usuarios en las reseñas de libros que tienen pocas opiniones, 
#    ya que la base de datos muestra que algunos libros no han sido ampliamente comentados.
# 
# 3. Las editoriales pueden enfocarse en la publicación de libros de mayor extensión, ya que Penguin Books, una de las 
#    editoriales más destacadas en esta base de datos, tiene muchos libros con más de 50 páginas.
# 
# 4. Para los autores, podría ser útil identificar a los usuarios más activos y comprometidos con las reseñas, como los 
#    que calificaron más de 50 libros, para establecer relaciones con ellos y mejorar la promoción de sus libros.
#
# Resumen general:
# Este análisis proporciona una visión general de la distribución de libros, autores, editoriales y la participación 
# de los usuarios en la base de datos. Los resultados indican que los libros recientes (post-2000) son los más comunes, 
# que algunas editoriales se destacan en publicaciones más largas y que algunos autores tienen un buen desempeño en 
# términos de calificación. Además, la actividad de los usuarios es clave para obtener una mejor comprensión de las 
# preferencias de lectura y las reseñas, lo que puede ayudar a los editores, autores y plataformas a mejorar la calidad 
# de su contenido y la experiencia de los usuarios.

# Fin del código
 
