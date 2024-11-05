from flask import Flask, request, jsonify
from google.cloud import datastore

app = Flask(__name__)
datastore_client = datastore.Client()

# Create a Business
# _1
@app.route('/businesses', methods=['POST'])
def create_business():
    # Get the request JSON data
    data = request.get_json()

    # Check for missing required fields
    required_fields = ['owner_id', 'name', 'street_address', 'city', 'state', 'zip_code']
    for field in required_fields:
        if field not in data:
            return jsonify({"Error": "The request body is missing at least one of the required attributes"}), 400

    # Create a new business entity in the Datastore
    business_key = datastore_client.key('Business')
    business_entity = datastore.Entity(key=business_key)
    business_entity.update({
        'owner_id': data['owner_id'],
        'name': data['name'],
        'street_address': data['street_address'],
        'city': data['city'],
        'state': data['state'],
        'zip_code': data['zip_code']
    })

    # Save the entity to Datastore
    datastore_client.put(business_entity)

    # Prepare the response
    response = {
        'id': business_entity.id,
        'owner_id': data['owner_id'],
        'name': data['name'],
        'street_address': data['street_address'],
        'city': data['city'],
        'state': data['state'],
        'zip_code': data['zip_code']
    }

    return jsonify(response), 201

# Get a Business
# _2
@app.route('/businesses/<int:business_id>', methods=['GET'])
def get_business(business_id):
    # Retrieve the business entity from Datastore
    key = datastore_client.key('Business', business_id)
    business = datastore_client.get(key)
    if business is None:
        # If not found, return 404 Not Found
        return jsonify({"Error": "No business with this business_id exists"}), 404
    else:
        # If found, return the business data
        business_data = {
            "id": business.key.id,
            "owner_id": business.get('owner_id'),
            "name": business.get('name'),
            "street_address": business.get('street_address'),
            "city": business.get('city'),
            "state": business.get('state'),
            "zip_code": business.get('zip_code')
        }
        return jsonify(business_data), 200

# List all Businesses
# _3
@app.route('/businesses', methods=['GET'])
def list_businesses():
    query = datastore_client.query(kind='Business')
    results = list(query.fetch())

    businesses = []
    for business in results:
        business_data = {
            "id": business.key.id,
            "owner_id": business.get('owner_id'),
            "name": business.get('name'),
            "street_address": business.get('street_address'),
            "city": business.get('city'),
            "state": business.get('state'),
            "zip_code": business.get('zip_code')
        }
        businesses.append(business_data)
    
    return jsonify(businesses), 200

# Edit a Business
# _4
@app.route('/businesses/<int:business_id>', methods=['PUT'])
def edit_business(business_id):
    data = request.get_json()

    # List of required attributes
    required_attributes = ['owner_id', 'name', 'street_address', 'city', 'state', 'zip_code']

    # Check if all required attributes are present
    if not data or not all(attr in data for attr in required_attributes):
        return jsonify({"Error": "The request body is missing at least one of the required attributes"}), 400

    # Retrieve the business entity from Datastore
    key = datastore_client.key('Business', business_id)
    business = datastore_client.get(key)
    if business is None:
        return jsonify({"Error": "No business with this business_id exists"}), 404

    # Update the business entity with new values
    business['owner_id'] = data['owner_id']
    business['name'] = data['name']
    business['street_address'] = data['street_address']
    business['city'] = data['city']
    business['state'] = data['state']
    business['zip_code'] = data['zip_code']

    # Save the updated entity back to Datastore
    datastore_client.put(business)

    # Prepare the response data
    business_data = {
        "id": business.key.id,
        "owner_id": business['owner_id'],
        "name": business['name'],
        "street_address": business['street_address'],
        "city": business['city'],
        "state": business['state'],
        "zip_code": business['zip_code']
    }

    return jsonify(business_data), 200

# Delete a Business
# _5
@app.route('/businesses/<int:business_id>', methods=['DELETE'])
def delete_business(business_id):
    key = datastore_client.key('Business', business_id)
    business = datastore_client.get(key)
    if business is None:
        return jsonify({"Error": "No business with this business_id exists"}), 404

    # Delete associated reviews
    review_query = datastore_client.query(kind='Review')
    review_query.add_filter('business_id', '=', business_id)
    reviews = list(review_query.fetch())
    for review in reviews:
        datastore_client.delete(review.key)

    # Delete the business
    datastore_client.delete(key)

    return '', 204

# List all Businesses for an Owner
# _6
@app.route('/owners/<int:owner_id>/businesses', methods=['GET'])
def list_businesses_for_owner(owner_id):
    query = datastore_client.query(kind='Business')
    query.add_filter('owner_id', '=', owner_id)
    results = list(query.fetch())

    businesses = []
    for business in results:
        business_data = {
            "id": business.key.id,
            "owner_id": business.get('owner_id'),
            "name": business.get('name'),
            "street_address": business.get('street_address'),
            "city": business.get('city'),
            "state": business.get('state'),
            "zip_code": business.get('zip_code')
        }
        businesses.append(business_data)
    
    return jsonify(businesses), 200

# Create a Review
# _7
@app.route('/reviews', methods=['POST'])
def create_review():
    data = request.get_json()

    # Required attributes
    required_attributes = ['user_id', 'business_id', 'stars']

    # Check if data is provided and all required attributes are present
    if not data or not all(attr in data for attr in required_attributes):
        return jsonify({"Error": "The request body is missing at least one of the required attributes"}), 400

    # Check if the business with 'business_id' exists
    business_key = datastore_client.key('Business', data['business_id'])
    business = datastore_client.get(business_key)
    if business is None:
        return jsonify({"Error": "No business with this business_id exists"}), 404

    # Check if a review already exists by this user for this business
    query = datastore_client.query(kind='Review')
    query.add_filter('user_id', '=', data['user_id'])
    query.add_filter('business_id', '=', data['business_id'])
    existing_reviews = list(query.fetch())
    if existing_reviews:
        return jsonify({"Error": "You have already submitted a review for this business. You can update your previous review, or delete it and submit a new review"}), 409

    # Create new Review entity
    key = datastore_client.key('Review')
    review_entity = datastore.Entity(key=key)
    review_entity.update({
        'user_id': data['user_id'],
        'business_id': data['business_id'],
        'stars': data['stars'],
        'review_text': data.get('review_text', '')
    })

    # Save the entity to datastore
    datastore_client.put(review_entity)

    # Prepare response data
    review_data = {
        'id': review_entity.key.id,
        'user_id': review_entity['user_id'],
        'business_id': review_entity['business_id'],
        'stars': review_entity['stars'],
        'review_text': review_entity.get('review_text', '')
    }

    return jsonify(review_data), 201

# Get a Review
# _8
@app.route('/reviews/<int:review_id>', methods=['GET'])
def get_review(review_id):
    key = datastore_client.key('Review', review_id)
    review = datastore_client.get(key)
    if review is None:
        return jsonify({"Error": "No review with this review_id exists"}), 404
    else:
        review_data = {
            "id": review.key.id,
            "user_id": review.get('user_id'),
            "business_id": review.get('business_id'),
            "stars": review.get('stars'),
            "review_text": review.get('review_text', '')
        }
        return jsonify(review_data), 200
    
# Edit a Review
# _9
@app.route('/reviews/<int:review_id>', methods=['PUT'])
def edit_review(review_id):
    data = request.get_json()

    # Check if data is provided and 'stars' attribute is present
    if not data or 'stars' not in data:
        return jsonify({"Error": "The request body is missing at least one of the required attributes"}), 400

    # Retrieve the review entity from Datastore
    key = datastore_client.key('Review', review_id)
    review = datastore_client.get(key)
    if review is None:
        return jsonify({"Error": "No review with this review_id exists"}), 404

    # Update the review entity with provided values (Partial update semantics)
    review['stars'] = data['stars']
    if 'review_text' in data:
        review['review_text'] = data['review_text']

    # Save the updated entity back to Datastore
    datastore_client.put(review)

    # Prepare response data
    review_data = {
        'id': review.key.id,
        'user_id': review.get('user_id'),
        'business_id': review.get('business_id'),
        'stars': review.get('stars'),
        'review_text': review.get('review_text', '')
    }

    return jsonify(review_data), 200

# Delete a Review
# _10
@app.route('/reviews/<int:review_id>', methods=['DELETE'])
def delete_review(review_id):
    key = datastore_client.key('Review', review_id)
    review = datastore_client.get(key)
    if review is None:
        return jsonify({"Error": "No review with this review_id exists"}), 404

    datastore_client.delete(key)
    return '', 204

# List all Reviews for a User
# _11
@app.route('/users/<int:user_id>/reviews', methods=['GET'])
def list_reviews_for_user(user_id):
    query = datastore_client.query(kind='Review')
    query.add_filter('user_id', '=', user_id)
    reviews = list(query.fetch())

    review_list = []
    for review in reviews:
        review_data = {
            "id": review.key.id,
            "user_id": review.get('user_id'),
            "business_id": review.get('business_id'),
            "stars": review.get('stars'),
            "review_text": review.get('review_text', '')
        }
        review_list.append(review_data)

    return jsonify(review_list), 200

if __name__ == '__main__':
    app.run(host='localhost', port=8080, debug=True)
