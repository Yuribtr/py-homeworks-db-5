import sqlalchemy
import csv


def read_query(filename):
    query_file = open(filename, mode='rt', encoding='utf-8')
    query_text = ''.join(query_file.readlines())
    query_file.close()
    return query_text


def read_data(filename):
    data_file = open(filename, mode='rt', encoding='utf-8')
    csv.register_dialect('MyDialect', delimiter=';')
    tmp = []
    reader = csv.DictReader(data_file, dialect='MyDialect')
    for item in reader:
        tmp.append(item)
    return tmp


DATA = read_data('demo-data.csv')

print('Connecting to DB...')
db = None
while True:
    try:
        db_name, login, psw = map(str, (input("""Введите через пробел имя базы, логин и пароль
или нажмите Enter чтобы заполнить их значениями 'test':""") or 'test test test').split())
        db = sqlalchemy.create_engine(f'postgresql://{login}:{psw}@localhost:5432/{db_name}')
        break
    except:
        continue
connection = db.connect()

print('Creating empty tables...')
connection.execute(read_query('queries/create-tables.sql'))

print('\nAdding musicians...')
query = read_query('queries/insert-musicians.sql')
res = connection.execute(query.format(','.join({f"('{x['musician']}')" for x in DATA})))
print(f'Inserted {res.rowcount} musicians.')

print('\nAdding genres...')
query = read_query('queries/insert-genres.sql')
res = connection.execute(query.format(','.join({f"('{x['genre']}')" for x in DATA})))
print(f'Inserted {res.rowcount} genres.')

print('\nLinking musicians with genres...')
# assume that musician + genre has to be unique
genres_musicians = {x['musician'] + x['genre']: [x['musician'], x['genre']] for x in DATA}
query = read_query('queries/insert-genre-musician.sql')
# this query can't be run in batch, so execute one by one
res = 0
for key, value in genres_musicians.items():
    res += connection.execute(query.format(value[1], value[0])).rowcount
print(f'Inserted {res} connections.')

print('\nAdding albums...')
# assume that albums has to be unique
albums = {x['album']: x['album_year'] for x in DATA}
query = read_query('queries/insert-albums.sql')
res = connection.execute(query.format(','.join({f"('{x}', '{y}')" for x, y in albums.items()})))
print(f'Inserted {res.rowcount} albums.')

print('\nLinking musicians with albums...')
# assume that musicians + album has to be unique
albums_musicians = {x['musician'] + x['album']: [x['musician'], x['album']] for x in DATA}
query = read_query('queries/insert-album-musician.sql')
# this query can't be run in batch, so execute one by one
res = 0
for key, values in albums_musicians.items():
    res += connection.execute(query.format(values[1], values[0])).rowcount
print(f'Inserted {res} connections.')

print('\nAdding tracks...')
query = read_query('queries/insert-track.sql')
# this query can't be run in batch, so execute one by one
res = 0
for item in DATA:
    res += connection.execute(query.format(item['track'], item['length'], item['album'])).rowcount
print(f'Inserted {res} tracks.')

print('\nAdding collections...')
query = read_query('queries/insert-collections.sql')
res = connection.execute(query.format(','.join({f"('{x['collection']}', {x['collection_year']})" for x in DATA if
                                                x['collection'] and x['collection_year']})))
print(f'Inserted {res.rowcount} collections.')

print('\nLinking collections with tracks...')
query = read_query('queries/insert-collection-track.sql')
# this query can't be run in batch, so execute one by one
res = 0
for item in DATA:
    res += connection.execute(query.format(item['collection'], item['track'])).rowcount
print(f'Inserted {res} connections.')

print('\nDatabase ready, let\'s have some fun...')

print('\nHow many musicians plays in each genres:')
query = read_query('queries/count-musicians-by-genres.sql')
res = connection.execute(query)
print(*res, sep='\n')

print('\nHow many tracks in all albums 2019-2020:')
query = read_query('queries/count-tracks-in-albums-by-year.sql')
res = connection.execute(query.format(2019, 2020))
print(*res, sep='\n')

print('\nAverage track length in each album:')
query = read_query('queries/count-average-tracks-by-album.sql')
res = connection.execute(query)
print(*res, sep='\n')

print('\nAll musicians that have no albums in 2020:')
query = read_query('queries/select-musicians-by-album-year.sql')
res = connection.execute(query.format(2020))
print(*res, sep='\n')

print('\nAll collections with musician Steve:')
query = read_query('queries/select-collection-by-musician.sql')
res = connection.execute(query.format('Steve'))
print(*res, sep='\n')

print('\nAlbums with musicians that play in more than 1 genre:')
query = read_query('queries/select-albums-by-genres.sql')
res = connection.execute(query.format(1))
print(*res, sep='\n')

print('\nAbsence tracks in collections:')
query = read_query('queries/select-absence-tracks-in-collections.sql')
res = connection.execute(query)
print(*res, sep='\n')

print('\nMusicians with shortest track length:')
query = read_query('queries/select-musicians-min-track-length.sql')
res = connection.execute(query)
print(*res, sep='\n')

print('\nAlbums with minimum number of tracks:')
query = read_query('queries/select-albums-with-minimum-tracks.sql')
res = connection.execute(query)
print(*res, sep='\n')
