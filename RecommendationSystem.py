from cassandra.cluster import Cluster
import csv
from cassandra.query import BatchStatement
from sklearn.metrics.pairwise import cosine_similarity
import random
from main import book_recommendations_indexes

# CREATE KEYSPACE data WITH replication = {'class':'SimpleStrategy', 'replication_factor' : 1};
# CREATE TABLE users (User-ID int PRIMARY KEY, Location text, Age int);


class RecommendationSystem:
    def __init__(self):
        self.cluster = Cluster(['127.0.0.1'], port=9042)
        self.session = self.cluster.connect("data")

    def insert_data(self):
        self.insert_users()
        self.insert_books()
        self.insert_book_ratings()

    def insert_users(self):
        batch = BatchStatement()
        with open('BX-Users.csv', 'r') as csvfile:
            csvreader = csv.DictReader(csvfile, delimiter=';')
            #print(csvreader.fieldnames)
            for row in csvreader:
                user_id = int(row['UserID'])
                location = row['Location']
                age = int(row['Age']) if row['Age'].isdigit() else None
                query = "INSERT INTO users (UserID, Location, Age) VALUES (%s, %s, %s);"
                batch.add(query, (user_id, location, age))
                if len(batch) > 200:
                    self.session.execute(batch)
                    batch.clear()
            if len(batch) > 0:
                self.session.execute(batch)

    def insert_book_ratings(self):
        batch = BatchStatement()
        with open('BX-Book-Ratings.csv', 'r') as csvfile:
            csvreader = csv.DictReader(csvfile, delimiter=';')

            for row in csvreader:
                user_id = int(row['User-ID'])
                isbn = row['ISBN']
                book_rating = int(row['Book-Rating'])
                query = "INSERT INTO book_ratings (UserID, ISBN, book_rating) VALUES (%s, %s, %s);"
                batch.add(query, (user_id, isbn, book_rating))
                if len(batch) > 500:
                    self.session.execute(batch)
                    batch.clear()
            if len(batch) > 0:
                self.session.execute(batch)

    def insert_books(self):
        batch = BatchStatement()
        with open('BX-Books.csv', 'r') as csvfile:
            csvreader = csv.DictReader(csvfile, delimiter=';')

            for row in csvreader:
                isbn = row['ISBN']
                book_title = row['Book-Title']
                book_author = row['Book-Author']
                year = int(row['Year-Of-Publication']) if row['Year-Of-Publication'].isdigit() else 0
                publisher = row['Publisher']
                imageS = row['Image-URL-S']
                imageM = row['Image-URL-M']
                imageL = row['Image-URL-L']
                query = "INSERT INTO books (ISBN, book_title, book_author, year_of_publication, publisher, Image_s,Image_m, Image_l) VALUES (%s, %s, %s, %s, %s, %s, %s, %s);"
                batch.add(query, (isbn, book_title, book_author, year, publisher, imageS, imageM, imageL))
                if len(batch) > 100:
                    self.session.execute(batch)
                    batch.clear()
            if len(batch) > 0:
                self.session.execute(batch)

    def insert_user(self, userid, location, age):
        if self.user_exists(userid):
            return -1
        else:
            age = int(age) if age.isdigit() else None
            location = location  if len(location)>0 else None
            query = "INSERT INTO users (UserID, Location, Age) VALUES (%s, %s, %s);"
            self.session.execute(query, (userid, location, age))
            return 0


    def user_exists(self, userid):
        query = "SELECT * FROM users WHERE userID = %s;"
        result = self.session.execute(query, (userid,)).all()
        if not result:
            return False
        else:
            return True

    def delete_all(self):
        query = "TRUNCATE books;"
        query2 = "TRUNCATE users;"
        query3 = "TRUNCATE book_ratings;"
        self.session.execute(query)
        self.session.execute(query2)
        self.session.execute(query3)

    def delete_user_ratings(self, userid):
        query = "DELETE FROM book_ratings WHERE userID =  %s;"
        self.session.execute(query, (userid,))

    def update_book_rating(self, userid, isbn, new_rating):
        query = f"UPDATE book_ratings SET book_rating = {new_rating} WHERE isbn = %s AND userID = {userid} ;"
        self.session.execute(query, (isbn,))

    def add_book_rating(self, userid, isbn, rating):
        if self.book_exists(isbn):
            if self.user_rating_exists(userid, isbn):
                return -2 #user already rated this book
            else:
                rating = int(rating)
                query = "INSERT INTO book_ratings (UserID, ISBN, book_rating) VALUES (%s, %s, %s);"
                self.session.execute(query, (userid, isbn, rating))
                return 0
        return -1

    def user_rating_exists(self,userid, isbn):
        query = "SELECT * from book_ratings WHERE userID = %s AND isbn = %s;"
        result = self.session.execute(query, (userid, isbn,)).all()
        if not result:
            return False
        return True
    def book_exists(self, isbn):
        query = "SELECT * FROM books WHERE isbn = %s;"
        result = self.session.execute(query, (isbn,)).all()
        if not result:
            return False
        else:
            return True

    def best_and_worst_books(self, userid):
        query = "SELECT * from book_ratings WHERE userID = %s;"
        user_ratings = self.session.execute(query, (userid,)).all()

        if not user_ratings:
            query = "SELECT * from users WHERE userID = %s;"
            user = self.session.execute(query, (userid,)).all()

            if not user:  # There is no such user
                return -1, None
            else:  # This user does not have ratings
                return -2, None

        sorted_user_ratings = sorted(user_ratings, key=lambda x: x.book_rating, reverse=True)
        best_rated_books = sorted_user_ratings[:2]
        sorted_user_ratings = sorted_user_ratings[2:]
        worst_rated_books = sorted_user_ratings[-2:]

        return best_rated_books, worst_rated_books

    def get_user_ids(self, best, worst):
        query_best = "SELECT userid FROM book_ratings WHERE isbn = %s and book_rating >= 6 ALLOW FILTERING;"
        query_worst = "SELECT userid FROM book_ratings WHERE  isbn = %s and book_rating < 6 ALLOW FILTERING;"

        ids = []
        for book in best:
            id = self.session.execute(query_best, (book.isbn,)).all()
            ids += id

        for book in worst:
            id = self.session.execute(query_worst, (book.isbn,)).all()
            ids += id

        return ids

    def custom_sort_key(self, isbn):
        return int(isbn) if isbn.isdigit() else float('inf'), isbn

    def create_user_item_matrix(self, user_ids):
        query = "SELECT * FROM book_ratings WHERE userid IN {};".format(tuple([i for i in user_ids]))
        ratings = self.session.execute(query).all()

        unique_users = sorted(set(user_ids))
        unique_books = sorted(set(rating.isbn for rating in ratings), key=self.custom_sort_key)

        len_users = len(unique_users)
        len_books = len(unique_books)
        user_item_matrix = [[0 for _ in range(len_books)] for _ in range(len_users)]

        # filling the matrix with user item matrix with book ratings
        for row in ratings:
            user_index = unique_users.index(row.userid)
            book_index = unique_books.index(row.isbn)
            user_item_matrix[user_index][book_index] = row.book_rating

        return user_item_matrix, unique_users, unique_books

    def get_recommended_books(self, recommended):
        query = "SELECT * FROM books WHERE isbn IN {};".format(tuple([str(i) for i in recommended]))
        books = self.session.execute(query).all()

        return books

    def get_general_recommendations(self):
        query = "SELECT * FROM book_ratings WHERE book_rating = 10 ALLOW FILTERING;"
        ratings = self.session.execute(query).all()

        random_indices = random.sample(range(len(ratings)), 9)
        isbn_list = []

        for index in random_indices:
            isbn_list.append(ratings[index].isbn)

        query2 = "SELECT * FROM books WHERE isbn IN {};".format(tuple([i for i in isbn_list]))
        books = self.session.execute(query2).all()

        return books

    def __del__(self):
        self.session.shutdown()



def get_recommendations(userid):
    rs = RecommendationSystem()
    user_id = int(userid)
    best, worst = rs.best_and_worst_books(user_id)
    if best == -1:
        return -1
    elif best == -2:
        return rs.get_general_recommendations()
    else:
        ids = rs.get_user_ids(best, worst)
        unique_ids = set(ids)
        user_ids = [row.userid for row in unique_ids]
        if user_id not in user_ids:
            user_ids.append(user_id)
        user_item_matrix, unique_users, unique_books = rs.create_user_item_matrix(user_ids)
        user_index = unique_users.index(user_id)
        books = book_recommendations_indexes(user_item_matrix, user_index)
        recommended_isbn = []

        for book in books:
            recommended_isbn.append(str(unique_books[book]))

        recommendations = rs.get_recommended_books(recommended_isbn)
        return recommendations


def addUser(userid, location, age):
    rs = RecommendationSystem()
    result = rs.insert_user(userid, location, age)
    return result

def addRating(userid, isbn, rating):
    rs = RecommendationSystem()
    result = rs.add_book_rating(userid, isbn, rating)
    return result
