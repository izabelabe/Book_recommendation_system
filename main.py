from sklearn.metrics.pairwise import cosine_similarity

#from timing import time_all, inserting_time
#if __name__ == '__main__':

#   recommendation_system = RedisRecommendationSystem()
#   inserting_time(recommendation_system)


def book_recommendations_indexes(user_item_matrix, user_index, neighbors=5, num_rec=7):
    similarity = cosine_similarity(user_item_matrix)
    user_similarity = similarity[user_index]
    similar_users = user_similarity.argsort()[::-1][1:neighbors + 1]   #the higher the more similar, but we are not taking into consideration the user itself
    user_ratings = user_item_matrix[user_index]
    predictions = user_ratings.copy()

    for book_index in range(len(user_ratings)):
        if user_ratings[book_index] == 0:  # taking into consideration only books unrated by the user
            rating_sum = 0
            similarity_sum = 0
            for neighbor_index in similar_users:
                neighbor_similarity = similarity[user_index][neighbor_index]
                neighbor_rating = user_item_matrix[neighbor_index][book_index]
                rating_sum += neighbor_similarity * neighbor_rating
                similarity_sum += neighbor_similarity

            predictions[book_index] = rating_sum / (similarity_sum + 1e-6)

    books_indices = sorted(range(len(predictions)), key=lambda i: predictions[i], reverse=True)
    unrated_books = [index for index in books_indices if user_ratings[index] == 0] # eliminating already rated books
    recommended = unrated_books[:num_rec]

    return recommended