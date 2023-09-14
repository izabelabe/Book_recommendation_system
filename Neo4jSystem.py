from neo4j import GraphDatabase
import csv
import random
from sklearn.metrics.pairwise import cosine_similarity
from main import book_recommendations_indexes

class Neo4jRecommendationSystem:
    def __init__(self):
        self.driver = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", "qwertyuiop"))

    def clean_data(self, input_filename, output_filename):

        with open(input_filename, 'r') as input_file, open(output_filename, 'w', encoding='utf-8') as output_file:
            for line in input_file:
                fields = line.strip().split(';')
                location = fields[1].replace('"', '').replace('\n', '').replace('\\', '')
                fields[1] = location
                output_line = ';'.join(fields) + '\n'
                output_file.write(output_line)

    def insert_data(self):
        self.insert_user_data()
        self.insert_book_data()
        self.insert_ratings_data()

    def insert_user_data(self):
        with self.driver.session() as session:
            users_query = """
            LOAD CSV WITH HEADERS FROM 'file:///cleaned_users.csv' AS row FIELDTERMINATOR ';'
            CREATE (:User {user_id: toInteger(row.UserID), location: row.Location, age: row.Age})
            """
            session.write_transaction(lambda tx: tx.run(users_query))

    def insert_ratings_data(self):
        with self.driver.session() as session:
            batch_size = 2000

            query = (
                "UNWIND $batch AS row "
                "MATCH (user:User {user_id: row.user_id}) "
                "MATCH (book:Book {isbn: row.isbn}) "
                "MERGE (user)-[r:RATED]->(book) "
                "ON CREATE SET r.rating = row.book_rating"
            )

            with open("BX-Book-Ratings-cleaned.csv", "r") as csvfile:
                csvreader = csv.DictReader(csvfile, delimiter=';')

                batch = []
                for row in csvreader:
                    user_id = int(row['User-ID'])
                    isbn = str(row['ISBN'])
                    book_rating = int(row['Book-Rating'])

                    rating_data = {
                        'user_id': user_id,
                        'isbn': isbn,
                        'book_rating': book_rating
                    }

                    batch.append(rating_data)

                    if len(batch) >= batch_size:
                        session.run(query, batch=batch)
                        batch = []

                if batch:
                    session.run(query, batch=batch)

    def insert_book_data(self):
        batch_size = 1000
        batch = []
        unique_isbns = set()
        query = (
            "UNWIND $ratings AS rating "
            "CREATE (:Book {isbn: rating.isbn})"
        )
        query2 = (
            "UNWIND $batch AS row "
            "MATCH (book:Book {isbn: row.isbn}) "
            "SET book.book_title = row.book_title, "
            "book.book_author = row.book_author, "
            "book.year_of_publication = row.year_of_publication, "
            "book.publisher = row.publisher, "
            "book.image_s = row.image_s, "
            "book.image_m = row.image_m, "
            "book.image_l = row.image_l"
        )
        with self.driver.session() as session:
            with open("BX-Book-Ratings-cleaned.csv", "r") as csvfile:
                csvreader = csv.DictReader(csvfile, delimiter=';')

                for row in csvreader:
                    isbn = str(row['ISBN'])
                    if isbn not in unique_isbns:
                        unique_isbns.add(isbn)
                        batch.append({'isbn': isbn})

                    if len(batch) >= batch_size:
                        session.run(query, ratings=batch)
                        batch = []
                if batch:
                    session.run(query, ratings=batch)
                    batch = []

            with open("BX-Books-cleaned.csv", "r") as csvfile:
                csvreader = csv.DictReader(csvfile, delimiter=';')

                for row in csvreader:
                    isbn = str(row['ISBN'])
                    book_title = row['Book-Title']
                    book_author = row['Book-Author']
                    year = int(row['Year-Of-Publication']) if row['Year-Of-Publication'].isdigit() else 0
                    publisher = row['Publisher']
                    imageS = row['Image-URL-S']
                    imageM = row['Image-URL-M']
                    imageL = row['Image-URL-L']

                    book_data = {
                        'isbn': isbn,
                        'book_title': book_title,
                        'book_author': book_author,
                        'year_of_publication': year,
                        'publisher': publisher,
                        'image_s': imageS,
                        'image_m': imageM,
                        'image_l': imageL
                    }
                    batch.append(book_data)

                    if len(batch) >= batch_size:
                        session.write_transaction(lambda tx: tx.run(query2, batch=batch))
                        batch = []

                if batch:
                    session.write_transaction(lambda tx: tx.run(query2, batch=batch))

    def delete_all(self):
        with self.driver.session() as session:
            labels = ["User", "Book"]
            for label in labels:
                query = f"MATCH (n:{label}) WITH n LIMIT 20000 DETACH DELETE n RETURN COUNT(n) AS deletedCount"
                while True:
                    result = session.write_transaction(lambda tx: tx.run(query).single())
                    deleted_count = result["deletedCount"]
                    if deleted_count == 0:
                        break

    def delete_user_ratings(self, userid):
        with self.driver.session() as session:
            query = "MATCH (:User {user_id: $userid})-[r:RATED]->() DELETE r"
            session.write_transaction(lambda tx: tx.run(query, userid=userid))

    def insert_user(self, userid, location, age):
        with self.driver.session() as session:
            query = "MATCH (user:User {user_id: $userid}) RETURN user"
            user = session.run(query, userid=userid).single()
            if user is None:
                query2 = "CREATE (:User {user_id: $user_id, location: $location, age: $age})"
                session.write_transaction(lambda tx: tx.run(query2, user_id=userid, location=location, age=age))
                return 0
            return -1

    def add_book_rating(self, userid, isbn, book_rating):
        with self.driver.session() as session:
            query = "MATCH (book:Book {isbn: $isbn}) RETURN book"
            book =  session.run(query, isbn=isbn).single()
            if book:
                #query2 = "MATCH (user:User {user_id: $user_id})-[r:RATED]->(book:Book {isbn: $isbn}) RETURN COUNT(r) AS count"
                query2 = (
                    "MATCH (user:User {user_id: $user_id})"
                    "MATCH (book:Book {isbn: $isbn})"
                    "RETURN EXISTS((user)-[:RATED]->(book)) AS already_rated"
                )
                rating = session.run(query2, user_id=userid, isbn=isbn).single()
                if rating["already_rated"] > 0:
                    return -2
                else:
                    query3 = (
                        "MATCH (user:User {user_id: $user_id})"
                        "MATCH (book:Book {isbn: $isbn})"
                        "MERGE (user)-[r:RATED]->(book)"
                        "ON CREATE SET r.rating = $book_rating"
                    )
                    session.write_transaction(lambda tx: tx.run(query3, user_id=userid, isbn=isbn, book_rating=book_rating))
                    return 0
            return -1
    def update_book_rating(self, userid, isbn, new_rating):
        with self.driver.session() as session:
            query = ("MATCH (user:User {user_id: $userid})-[r:RATED]->(book:Book {isbn: $isbn})"
                     "SET r.rating = $new_rating "
                     "RETURN user, r, book")
            session.write_transaction(
                lambda tx: tx.run(query, userid=userid, isbn=isbn, new_rating=new_rating).single())

    def best_and_worst_books(self, userid):
        with self.driver.session() as session:
            query = (
                "MATCH (user:User {user_id: $userid})-[r:RATED]->(book:Book)"
                "RETURN r AS rating, book"
            )

            ratings = session.run(query, userid=userid)
            ratings_list = []

            for rating in ratings:
                ratings_list.append({
                    "rating": rating["rating"]["rating"],
                    "isbn": rating["book"]["isbn"]
                })

            if not ratings_list:
                query2 = "MATCH (user:User {user_id: $userid}) RETURN user"
                user = session.run(query2, userid=userid).single()
                if user is None:
                    return -1, None
                else:
                    return -2, None

            sorted_user_ratings = sorted(ratings_list, key=lambda x: (x["isbn"]), reverse=False)
            sorted_user_ratings = sorted(sorted_user_ratings, key=lambda x: (x["rating"]), reverse=True)

            best_rated_books = sorted_user_ratings[:2]
            sorted_user_ratings = sorted_user_ratings[2:]
            worst_rated_books = sorted_user_ratings[-2:]

            return best_rated_books, worst_rated_books

    def get_user_ids(self, best, worst):
        with self.driver.session() as session:
            query_best = ("MATCH (book:Book {isbn: $isbn})<-[r:RATED]-(user:User) "
                          "WHERE r.rating >= 6 "
                          "RETURN user.user_id AS userid")

            query_worst = ("MATCH (book:Book {isbn: $isbn})<-[r:RATED]-(user:User) "
                           "WHERE r.rating <= 5 "
                           "RETURN user.user_id AS userid")

            ids = []

            for book in best:
                result = session.run(query_best, isbn=book["isbn"])
                for record in result:
                    ids += record

            for book in worst:
                result = session.run(query_worst, isbn=book["isbn"])
                for record in result:
                    ids += record

            ids = list(set(ids))  # deleting duplicates
            return ids

    def get_user_info(self, user_id):
        with self.driver.session() as session:
            query = "MATCH (u:User {user_id: $user_id}) RETURN u"
            result = session.run(query,
                                 user_id=user_id)  # run without the write_transaction because it's a read only operation
            user_info = None
            for record in result:
                user_info = record['u']
                break
            return user_info.items()

    def custom_sort_key(self, isbn):
        return int(isbn) if isbn.isdigit() else float('inf'), isbn

    def create_user_item_matrix(self, user_ids):
        with self.driver.session() as session:
            query = ("MATCH (user:User)-[r:RATED]->(book:Book) "
                     "WHERE user.user_id IN $user_ids "
                     "RETURN user.user_id AS userid, book.isbn AS isbn, r.rating AS rating")

            ratings = list(session.run(query, user_ids=user_ids))

            unique_users = sorted(set(user_ids))
            unique_books = sorted(set(record["isbn"] for record in ratings), key=self.custom_sort_key)

            len_users = len(unique_users)
            len_books = len(unique_books)
            user_item_matrix = [[0 for _ in range(len_books)] for _ in range(len_users)]

            for rating in ratings:
                user_index = unique_users.index(rating["userid"])
                book_index = unique_books.index(rating["isbn"])
                user_item_matrix[user_index][book_index] = rating["rating"]

            return user_item_matrix, unique_users, unique_books

    def get_recommended_books(self, recommended):
        with self.driver.session() as session:
            query = "MATCH (book:Book) WHERE book.isbn IN $recommended RETURN book"
            recommended_books = []
            result = session.run(query, recommended=recommended)
            for record in result:
                recommended_books.append(dict(record["book"].items()))

        return recommended_books

    def get_general_recommendations(self):
        with self.driver.session() as session:
            query = "MATCH (user:User)-[r:RATED]->(book:Book) WHERE r.rating = 10 RETURN DISTINCT book.isbn AS isbn"
            ratings = session.run(query)
            isbns = [record["isbn"] for record in ratings]
            top_rated_isbns = random.sample(isbns, 9)
            query2 = "MATCH (book:Book) WHERE book.isbn IN $top_rated_isbns  RETURN book"
            recommended_books = []
            books = session.run(query2, top_rated_isbns=top_rated_isbns)
            for record in books:
                recommended_books.append(dict(record["book"].items()))
        return recommended_books

    def close(self):
        self.driver.close()




def get_recommendations_neo4j(userid):
    rs = Neo4jRecommendationSystem()
    user_id = int(userid)
    best, worst = rs.best_and_worst_books(user_id)
    if best == -1:
        return -1
    elif best == -2:
        return rs.get_general_recommendations()
    else:
        ids = rs.get_user_ids(best, worst)
        if user_id not in ids:
            ids.append(user_id)
        user_item_matrix, unique_users, unique_books = rs.create_user_item_matrix(ids)
        target_user_index = unique_users.index(user_id)
        book_indexes = book_recommendations_indexes(user_item_matrix, target_user_index)

        recommended_isbn = []
        for book in book_indexes:
            recommended_isbn.append(str(unique_books[book]))

        recommendations = rs.get_recommended_books(recommended_isbn)
        final_recommendations = [book for book in recommendations if 'book_title' in book]

        return final_recommendations


def addUserNeo4j(userid, location, age):
    rs = Neo4jRecommendationSystem()
    result = rs.insert_user(userid, location, age)
    return result


def addRatingNeo4j(userid, isbn, rating):
    rs = Neo4jRecommendationSystem()
    result = rs.add_book_rating(userid, isbn, rating)
    return result