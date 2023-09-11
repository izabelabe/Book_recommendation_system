import redis
import csv
from sklearn.metrics.pairwise import cosine_similarity
import random
from main import book_recommendations_indexes
redis_host = 'localhost'
redis_port = 6379


class RedisRecommendationSystem:
    def __init__(self):
        self.r = redis.Redis(host=redis_host, port=redis_port, decode_responses=True)

    def insert_data(self):
        self.insert_user_data()
        self.insert_book_data()
        self.insert_ratings_data()

    def insert_user_data(self):
        pipeline = self.r.pipeline()
        with open("BX-Users.csv", 'r') as csv_file:
            csv_reader = csv.DictReader(csv_file, delimiter=';')
            for row in csv_reader:
                user_id = row['UserID']
                location = row['Location']
                age = row['Age']

                pipeline.hset(user_id, 'Location', location)
                pipeline.hset(user_id, 'Age', age)

                if len(pipeline.command_stack) > 250:
                    pipeline.execute()

            pipeline.execute()

    def insert_book_data(self):
        with open("BX-Books.csv", 'r') as csv_file:
            csv_reader = csv.DictReader(csv_file, delimiter=';')
            pipeline = self.r.pipeline()
            for row in csv_reader:
                isbn = row['ISBN']
                #isbn = isbn.zfill(10)
                isbn = isbn[::-1].zfill(10)[::-1]
                book_data = {
                    'book_title': row['Book-Title'],
                    'book_author': row['Book-Author'],
                    'year_of_publication': int(row['Year-Of-Publication']) if row['Year-Of-Publication'].isdigit() else 0,
                    'publisher': row['Publisher'],
                    'image_s': row['Image-URL-S'],
                    'image_m': row['Image-URL-M'],
                    'image_l': row['Image-URL-L']
                }

                for field, value in book_data.items():
                    if value is not None:
                        pipeline.hset(isbn, field, value)

                        if len(pipeline.command_stack) > 250:
                            pipeline.execute()

            pipeline.execute()

    def insert_ratings_data(self):
        pipeline = self.r.pipeline()
        with open("BX-Book-Ratings.csv", 'r') as csv_file:
            csv_reader = csv.DictReader(csv_file, delimiter=';')
            for row in csv_reader:
                user_id = int(row['User-ID'])
                isbn = row['ISBN']
                isbn = isbn[::-1].zfill(10)[::-1]
                book_rating = int(row['Book-Rating'])

                pipeline.zadd(f"ratings:user:{user_id}", {isbn: book_rating})
                pipeline.zadd(f"ratings:isbn:{isbn}", {user_id: book_rating})  # to make finding user ids faster

                if len(pipeline.command_stack) > 250:
                    pipeline.execute()

            pipeline.execute()

    def delete_all(self):
        self.r.flushdb()

    def insert_user(self, userid, location, age):
        if self.r.exists(userid):
            return -1
        self.r.hset(userid, 'Location', location)
        self.r.hset(userid, 'Age', age)
        return 0

    def add_book_rating(self, userid, isbn, rating):
        isbn = isbn[::-1].zfill(10)[::-1]
        if self.r.exists(isbn):
            score = self.r.zscore(f"ratings:user:{userid}", isbn)
            if score is not None:
                return -2 #user already rated this book
            else:
                self.r.zadd(f"ratings:user:{userid}", {isbn: rating})
                self.r.zadd(f"ratings:isbn:{isbn}", {userid: rating})
                return 0
        return -1

    def delete_user_ratings(self, userid):
            redis_key = f"ratings:user:{userid}"
            isbns = self.r.zrange(redis_key, 0, -1)

            self.r.zremrangebyscore(redis_key, "-inf", "+inf")

            for isbn in isbns:
                self.r.zrem(f"ratings:isbn:{isbn}", userid)  # Delete from ISBN's sorted set

    def update_book_rating(self, user_id, isbn, new_rating):
        isbn = isbn[::-1].zfill(10)[::-1]
        redis_key_user = f"ratings:user:{user_id}"
        redis_key_isbn = f"ratings:isbn:{isbn}"
        self.r.zadd(redis_key_user, {isbn: new_rating})
        self.r.zadd(redis_key_isbn, {user_id: new_rating})

    def best_and_worst_books(self, userid):
        redis_key = f"ratings:user:{userid}"

        user_ratings = self.r.zrange(redis_key, 0, -1, withscores=True)
        sorted_user_ratings = sorted(user_ratings, key=lambda x: x[1], reverse=True)

        best_rated_books = sorted_user_ratings[:2]
        worst_rated_books = sorted_user_ratings[-2:]

        if not best_rated_books:
            if self.r.exists(userid):
                return -2, None  # user exists, but has no ratings
            else:
                return -1, None  # user does not exist

        best_isbns = [book[0] for book in best_rated_books]
        worst_isbns = [book[0] for book in worst_rated_books]

        common_books = set(best_isbns) & set(worst_isbns)

        if common_books:
            worst_rated_books = [book for book in worst_rated_books if book[0] not in common_books]

        return best_rated_books, worst_rated_books

    def get_user_ids(self, best, worst):
        ids = set()

        for book_isbn, _ in best:
            user_ids = self.r.zrangebyscore(f"ratings:isbn:{book_isbn}", 6, "+inf")
            ids.update(user_ids)

        for book_isbn, _ in worst:
            user_ids = self.r.zrangebyscore(f"ratings:isbn:{book_isbn}", "-inf", 6)
            ids.update(user_ids)

        return ids

    def custom_sort_key(self, isbn):
        return int(isbn) if isbn.isdigit() else float('inf'), isbn

    def create_user_item_matrix(self, user_ids):
        user_ids = [int(user_id) for user_id in user_ids]
        unique_users = sorted(set(user_ids))
        unique_books = set()

        user_to_index = {user_id: index for index, user_id in enumerate(unique_users)}

        for user_id in unique_users:
            redis_key = f"ratings:user:{user_id}"
            user_ratings = self.r.zrange(redis_key, 0, -1, withscores=True)
            unique_books.update(isbn for isbn, _ in user_ratings)

        unique_books = sorted(unique_books, key=self.custom_sort_key)
        len_users = len(unique_users)
        len_books = len(unique_books)

        user_item_matrix = [[0 for _ in range(len_books)] for _ in range(len_users)]

        for user_id in unique_users:
            redis_key = f"ratings:user:{user_id}"
            user_ratings = self.r.zrange(redis_key, 0, -1, withscores=True)
            user_index = user_to_index[user_id]
            for isbn, rating in user_ratings:
                book_index = unique_books.index(isbn)
                user_item_matrix[user_index][book_index] = int(rating)

        return user_item_matrix, unique_users, unique_books

    def get_recommended_books(self, recommended):
        books = []

        for isbn in recommended:
            book_data = self.r.hgetall(isbn)
            if book_data:
                books.append(book_data)

        return books


    def get_general_recommendations(self):
        redis_key_pattern = "ratings:user:*"

        user_keys = self.r.keys(redis_key_pattern)
        isbn_ratings = {}

        for user_key in user_keys:
            book_ratings = self.r.zrangebyscore(user_key, 10, 10)
            for isbn in book_ratings:
                isbn_ratings[isbn] = True

        # Choose 10 random ISBNs
        random_isbns = random.sample(list(isbn_ratings.keys()), min(10, len(isbn_ratings)))
        books = self.get_recommended_books(random_isbns)
        return books

    def get_rating_for_book(self, user_id, isbn):
        #isbn = isbn.zfill(10)
        isbn = isbn[::-1].zfill(10)[::-1]
        redis_key = f"ratings:user:{user_id}"
        rating = self.r.zscore(redis_key, isbn)
        if rating is not None:
            print(f"Rating for ISBN {isbn}: {rating}")
        else:
            print(f"Rating not found for ISBN {isbn}")


def check_user(userid):
    r = redis.Redis(host=redis_host, port=redis_port, decode_responses=True)
    location = r.hget(userid, 'Location')
    age = r.hget(userid, 'Age')

    if location is not None and age is not None:
        print(f"User {userid} data:")
        print(f"Location: {location}")
        print(f"Age: {age}")

    else:
        print(f"User {userid} not found.")


def retrieve_book_data(isbn):
    #isbn = isbn.zfill(10)
    isbn = isbn[::-1].zfill(10)[::-1]
    r = redis.Redis(host=redis_host, port=redis_port, decode_responses=True)
    book_data = r.hgetall(isbn)
    print(book_data)



def get_recommendations_redis(userid):
    rrs = RedisRecommendationSystem()
    user_id = int(userid)
    best, worst = rrs.best_and_worst_books(user_id)
    if best == -1:
        return -1
    elif best == -2:
        return rrs.get_general_recommendations()
    else:
        ids = rrs.get_user_ids(best, worst)
        if userid not in ids:
            ids.add(user_id)
        user_item_matrix, unique_users, unique_books = rrs.create_user_item_matrix(ids)

        unique_users = [int(item) for item in unique_users]

        target_user_index = unique_users.index(user_id)
        book_indexes = book_recommendations_indexes(user_item_matrix, target_user_index)

        recommended_isbn = []
        for book in book_indexes:
            recommended_isbn.append(str(unique_books[book]))

        recommendations = rrs.get_recommended_books(recommended_isbn)

        return recommendations


def addUserRedis(userid, location, age):
    rrs = RedisRecommendationSystem()
    result = rrs.insert_user(userid, location, age)
    return result


def addRatingRedis(userid, isbn, rating):
    rrs = RedisRecommendationSystem()
    result = rrs.add_book_rating(userid, isbn, rating)
    return result