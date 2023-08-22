from pymongo import MongoClient
import csv
from sklearn.metrics.pairwise import cosine_similarity
import random

class MongoRecommendationSystem:
    def __init__(self):
        self.client = MongoClient('mongodb://localhost:27017/')
        self.db = self.client["rec_system"]
        self.user_collection = self.db["users"]
        self.rating_collection = self.db["ratings"]
        self.book_collection = self.db["books"]


    def insert_data(self):
        self.insert_user_data()
        self.insert_book_data()
        self.insert_ratings_data()


    def insert_user_data(self):
        users = []
        with open("BX-Users.csv", 'r') as csv_file:
            csv_reader = csv.DictReader(csv_file, delimiter=';')
            for row in csv_reader:
                user_id = row['UserID']
                location = row['Location']
                age = row['Age']

                user_data = {
                    'user_id': user_id,
                    'location': location,
                    'age': age
                }

                users.append(user_data)

        self.user_collection.insert_many(users)


    def insert_ratings_data(self):
        ratings = []
        with open('BX-Book-Ratings.csv', 'r') as csvfile:
            csvreader = csv.DictReader(csvfile, delimiter=';')

            for row in csvreader:
                user_id = int(row['User-ID'])
                isbn = row['ISBN']
                book_rating = int(row['Book-Rating'])


                rating_data = {
                    'user_id': user_id,
                    'isbn': str(isbn),
                    'book_rating': book_rating
                }

                ratings.append(rating_data)

        self.rating_collection.insert_many(ratings)

    def insert_book_data(self):
        books = []
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

                book_data = {
                    'isbn' : str(isbn),
                    'book_title' : book_title,
                    'book_author' : book_author,
                    'year_of_publication' : year,
                    'publisher' : publisher,
                    'image_s' : imageS,
                    'image_m' : imageM,
                    'image_l': imageL
                }

                books.append(book_data)
        self.book_collection.insert_many(books)

    def delete_all(self):
        self.user_collection.delete_many({})
        self.book_collection.delete_many({})
        self.rating_collection.delete_many({})


    def delete_user_ratings(self, userid):
        self.rating_collection.delete_many({'user_id': userid})


    def update_book_rating(self, userid, isbn, new_rating):
        self.rating_collection.update_one({'user_id': userid, 'isbn': isbn}, {'$set': {'book_rating': new_rating}})


    def best_and_worst_books(self, userid):
        user_ratings = self.rating_collection.find({'user_id': userid})
        user_ratings = list(user_ratings)

        if not user_ratings:
            user = self.user_collection.find({'user_id': userid})
            if not user:
                return -1, None
            else:
                return -2, None

        sorted_user_ratings = sorted(user_ratings, key=lambda x: x['book_rating'], reverse=True)
        best_rated_books = sorted_user_ratings[:2]
        sorted_user_ratings = sorted_user_ratings[2:]
        worst_rated_books = sorted_user_ratings[-2:]

        return best_rated_books, worst_rated_books


    def get_user_ids(self, best, worst):
        best_isbns = [book['isbn'] for book in best]
        worst_isbns = [book['isbn'] for book in worst]
        ids = []

        ids += self.rating_collection.distinct('user_id', {'isbn': {'$in': best_isbns}, 'book_rating': {'$gte': 6}})

        ids += self.rating_collection.distinct('user_id', {'isbn': {'$in': worst_isbns}, 'book_rating': {'$lt': 6}})

        return ids

    def custom_sort_key(self, isbn):
        return int(isbn) if isbn.isdigit() else float('inf'), isbn
    def create_user_item_matrix(self, user_ids):
        unique_users = sorted(set(user_ids))
        unique_books = set()

        for user_id in unique_users:
            user_ratings = self.rating_collection.find({'user_id': user_id})
            unique_books.update(rating['isbn'] for rating in user_ratings)

        user_to_index = {user_id: index for index, user_id in enumerate(unique_users)}
        num_users = len(unique_users)
        num_books = len(unique_books)
        user_item_matrix = [[0 for _ in range(num_books)] for _ in range(num_users)]
        unique_books = sorted(unique_books, key=self.custom_sort_key)

        for user_id in unique_users:
            user_ratings = self.rating_collection.find({'user_id': user_id})
            user_index = user_to_index[user_id]
            for rating in user_ratings:
                book_index = unique_books.index(rating['isbn'])
                user_item_matrix[user_index][book_index] = rating['book_rating']

        return user_item_matrix, unique_users, unique_books

    def __del__(self):
        self.client.close()

    def get_recommended_books(self, recommended):
        recommended_books = self.book_collection.find({'isbn': {'$in': recommended}})
        return list(recommended_books)

    def get_general_recommendations(self):
        top_rated_ratings = self.rating_collection.find({'book_rating': 10})
        top_rated_isbns = random.sample([rating['isbn'] for rating in top_rated_ratings], 9)

        recommended_books = self.book_collection.find({'isbn': {'$in': top_rated_isbns}})

        return list(recommended_books)


def book_recommendations_indexes(user_item_matrix, target_user_index, num_neighbors=5, num_recommendations=7):
    user_similarity = cosine_similarity(user_item_matrix)
    target_user_similarity = user_similarity[target_user_index]
    most_similar_users_indices = target_user_similarity.argsort()[::-1][1:num_neighbors + 1]

    target_user_ratings = user_item_matrix[target_user_index]
    predicted_ratings = target_user_ratings.copy()

    for book_index in range(len(target_user_ratings)):
        if target_user_ratings[book_index] == 0:  # taking into consideration only books unrated by the user
            rating_sum = 0
            similarity_sum = 0

            for neighbor_index in most_similar_users_indices:
                neighbor_similarity = user_similarity[target_user_index][neighbor_index]
                neighbor_rating = user_item_matrix[neighbor_index][book_index]

                rating_sum += neighbor_similarity * neighbor_rating
                similarity_sum += neighbor_similarity

            predicted_ratings[book_index] = rating_sum / (similarity_sum + 1e-6)  # Avoid division by zero

    recommended_books_indices = sorted(range(len(predicted_ratings)), key=lambda i: predicted_ratings[i],
                                       reverse=True)
    # eliminating already rated books
    unrated_books_indices = [book_index for book_index in recommended_books_indices if
                             target_user_ratings[book_index] == 0]
    recommended_books = unrated_books_indices[:num_recommendations]

    return recommended_books


def get_recommendations_mongo(userid):
    rs = MongoRecommendationSystem()
    user_id = int(userid)
    best, worst = rs.best_and_worst_books(user_id)
    if best == -1:
        return -1
    elif best == -2:
        return rs.get_general_recommendations()
    else:
        ids = rs.get_user_ids(best, worst)
        if userid not in ids:
            ids.append(user_id)
        user_item_matrix, unique_users, unique_books = rs.create_user_item_matrix(ids)

        target_user_index = unique_users.index(user_id)
        book_indexes = book_recommendations_indexes(user_item_matrix, target_user_index)

        recommended_isbn = []
        for book in book_indexes:
            recommended_isbn.append(str(unique_books[book]))

        recommendations = rs.get_recommended_books(recommended_isbn)
        return recommendations


