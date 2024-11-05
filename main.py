from __future__ import annotations

import logging
import os

from flask import Flask, request

import sqlalchemy

from connect_connector import connect_with_connector

BUSINESSES = 'businesses'
REVIEWS = 'reviews'
USERS = 'users'
ERROR_NOT_FOUND_REVIEW = {'Error': 'No review with this review_id exists'}
ERROR_NOT_FOUND_BUSINESS = {'Error': 'No business with this business_id exists'}
ERROR_MISSING_ATTRIBUTES = {'Error': 'The request body is missing at least one of the required attributes'}
ERROR_CONFLICT_REVIEW = {'Error': 'You have already submitted a review for this business. You can update your previous review, or delete it and submit a new review'}
OWNERS = 'owners'

app = Flask(__name__)

logger = logging.getLogger()

# Sets up connection pool for the app
def init_connection_pool() -> sqlalchemy.engine.base.Engine:
    if os.environ.get('INSTANCE_CONNECTION_NAME'):
        return connect_with_connector()
        
    raise ValueError(
        'Missing database connection type. Please define INSTANCE_CONNECTION_NAME'
    )

# This global variable is declared with a value of `None`
db = None

# Initiates connection to database
def init_db():
    global db
    db = init_connection_pool()

def create_tables(db: sqlalchemy.engine.base.Engine) -> None:
    with db.connect() as conn:
        # Drop existing tables to avoid conflicts
        conn.execute(sqlalchemy.text('DROP TABLE IF EXISTS reviews;'))
        conn.execute(sqlalchemy.text('DROP TABLE IF EXISTS businesses;'))
        
        # Create businesses table with id as UNSIGNED INTEGER
        conn.execute(
            sqlalchemy.text(
                'CREATE TABLE IF NOT EXISTS businesses ('
                'id INTEGER UNSIGNED NOT NULL AUTO_INCREMENT, '
                'owner_id INTEGER NOT NULL, '
                'name VARCHAR(50) NOT NULL, '
                'street_address VARCHAR(100) NOT NULL, '
                'city VARCHAR(50) NOT NULL, '
                'state VARCHAR(2) NOT NULL, '
                'zip_code VARCHAR(10) NOT NULL, '  # Changed to VARCHAR for zip codes
                'PRIMARY KEY (id) );'
            )
        )
        # Create reviews table with business_id as UNSIGNED INTEGER
        conn.execute(
            sqlalchemy.text(
                'CREATE TABLE IF NOT EXISTS reviews ('
                'id INTEGER UNSIGNED NOT NULL AUTO_INCREMENT, '
                'user_id INTEGER NOT NULL, '
                'business_id INTEGER UNSIGNED NOT NULL, '
                'stars INTEGER NOT NULL CHECK (stars >= 0 AND stars <= 5), '  # Added CHECK constraint
                'review_text VARCHAR(1000), '
                'PRIMARY KEY (id), '
                'FOREIGN KEY (business_id) REFERENCES businesses(id) ON DELETE CASCADE, '
                'UNIQUE (user_id, business_id) );'
            )
        )
        conn.commit()



@app.route('/')
def index():
    return 'Please navigate to /businesses or /reviews to use this API'

# Create a business
@app.route('/' + BUSINESSES, methods=['POST'])
def post_businesses():
    content = request.get_json()

    required_attributes = ['owner_id', 'name', 'street_address', 'city', 'state', 'zip_code']

    # Check if any required attribute is missing
    if not content or not all(attr in content for attr in required_attributes):
        return ERROR_MISSING_ATTRIBUTES, 400

    try:
        with db.connect() as conn:
            # Insert the new business into the database
            stmt = sqlalchemy.text(
                'INSERT INTO businesses (owner_id, name, street_address, city, state, zip_code) '
                'VALUES (:owner_id, :name, :street_address, :city, :state, :zip_code)'
            )
            conn.execute(stmt, parameters={
                'owner_id': content['owner_id'],
                'name': content['name'],
                'street_address': content['street_address'],
                'city': content['city'],
                'state': content['state'],
                'zip_code': content['zip_code']
            })
            # Get the last inserted id
            stmt2 = sqlalchemy.text('SELECT LASTVAL()')
            business_id = conn.execute(stmt2).scalar()
            conn.commit()

            # Construct the 'self' URL
            business_url = request.url_root.strip('/') + '/businesses/' + str(business_id)

            response_body = {
                'id': business_id,
                'owner_id': content['owner_id'],
                'name': content['name'],
                'street_address': content['street_address'],
                'city': content['city'],
                'state': content['state'],
                'zip_code': content['zip_code'],
                'self': business_url
            }
            return response_body, 201
    except Exception as e:
        logger.exception(e)
        return {'Error': 'Unable to create business'}, 500

# Get all businesses with optional pagination
@app.route('/' + BUSINESSES, methods=['GET'])
def get_businesses():
    offset = request.args.get('offset', default=0, type=int)
    limit = request.args.get('limit', default=3, type=int)

    with db.connect() as conn:
        # Fetch total number of businesses
        total_stmt = sqlalchemy.text('SELECT COUNT(*) FROM businesses')
        total = conn.execute(total_stmt).scalar()

        # Fetch businesses with offset and limit
        stmt = sqlalchemy.text(
            'SELECT id, owner_id, name, street_address, city, state, zip_code '
            'FROM businesses ORDER BY id LIMIT :limit OFFSET :offset'
        )
        rows = conn.execute(stmt, parameters={'limit': limit, 'offset': offset})

        businesses = []
        for row in rows:
            business = row._asdict()
            business['self'] = request.url_root.strip('/') + '/businesses/' + str(business['id'])
            businesses.append(business)

        response_body = {'entries': businesses}

        # Calculate next offset
        next_offset = offset + limit
        if next_offset < total:
            next_url = request.url_root.strip('/') + '/businesses?offset={}&limit={}'.format(next_offset, limit)
            response_body['next'] = next_url

        return response_body, 200

# Get a business
@app.route('/' + BUSINESSES + '/<int:business_id>', methods=['GET'])
def get_business(business_id):
    with db.connect() as conn:
        stmt = sqlalchemy.text(
            'SELECT id, owner_id, name, street_address, city, state, zip_code '
            'FROM businesses WHERE id = :business_id'
        )
        row = conn.execute(stmt, parameters={'business_id': business_id}).one_or_none()
        if row is None:
            return ERROR_NOT_FOUND_BUSINESS, 404
        else:
            business = row._asdict()
            business['self'] = request.url
            return business, 200

# Edit a business
@app.route('/' + BUSINESSES + '/<int:business_id>', methods=['PUT'])
def put_business(business_id):
    content = request.get_json()

    required_attributes = ['owner_id', 'name', 'street_address', 'city', 'state', 'zip_code']

    # Check if any required attribute is missing
    if not content or not all(attr in content for attr in required_attributes):
        return ERROR_MISSING_ATTRIBUTES, 400

    with db.connect() as conn:
        # Check if the business exists
        stmt_check = sqlalchemy.text('SELECT id FROM businesses WHERE id = :business_id')
        row = conn.execute(stmt_check, parameters={'business_id': business_id}).one_or_none()

        if row is None:
            return ERROR_NOT_FOUND_BUSINESS, 404
        else:
            # Update the business
            stmt = sqlalchemy.text(
                'UPDATE businesses SET '
                'owner_id = :owner_id, '
                'name = :name, '
                'street_address = :street_address, '
                'city = :city, '
                'state = :state, '
                'zip_code = :zip_code '
                'WHERE id = :business_id'
            )
            conn.execute(stmt, parameters={
                'owner_id': content['owner_id'],
                'name': content['name'],
                'street_address': content['street_address'],
                'city': content['city'],
                'state': content['state'],
                'zip_code': content['zip_code'],
                'business_id': business_id
            })
            conn.commit()

            # Construct the 'self' URL
            business_url = request.url_root.strip('/') + '/businesses/' + str(business_id)

            response_body = {
                'id': business_id,
                'owner_id': content['owner_id'],
                'name': content['name'],
                'street_address': content['street_address'],
                'city': content['city'],
                'state': content['state'],
                'zip_code': content['zip_code'],
                'self': business_url
            }
            return response_body, 200

# Delete a business
@app.route('/' + BUSINESSES + '/<int:business_id>', methods=['DELETE'])
def delete_business(business_id):
    with db.connect() as conn:
        # Optionally delete related reviews here if implemented
        stmt = sqlalchemy.text('DELETE FROM businesses WHERE id = :business_id')
        result = conn.execute(stmt, parameters={'business_id': business_id})
        conn.commit()

        if result.rowcount == 1:
            return '', 204
        else:
            return ERROR_NOT_FOUND_BUSINESS, 404

# List all businesses for an owner
@app.route('/' + OWNERS + '/<int:owner_id>/businesses', methods=['GET'])
def get_owner_businesses(owner_id):
    with db.connect() as conn:
        stmt = sqlalchemy.text(
            'SELECT id, owner_id, name, street_address, city, state, zip_code '
            'FROM businesses WHERE owner_id = :owner_id'
        )
        rows = conn.execute(stmt, parameters={'owner_id': owner_id})

        businesses = []
        for row in rows:
            business = row._asdict()
            business['self'] = request.url_root.strip('/') + '/businesses/' + str(business['id'])
            businesses.append(business)

        return businesses, 200

# Create a review
@app.route('/' + REVIEWS, methods=['POST'])
def post_reviews():
    content = request.get_json()

    required_attributes = ['user_id', 'business_id', 'stars']

    # Check if any required attribute is missing
    if not content or not all(attr in content for attr in required_attributes):
        return ERROR_MISSING_ATTRIBUTES, 400

    # Check if the business exists
    with db.connect() as conn:
        stmt = sqlalchemy.text('SELECT id FROM businesses WHERE id = :business_id')
        business = conn.execute(stmt, parameters={'business_id': content['business_id']}).one_or_none()
        if business is None:
            return ERROR_NOT_FOUND_BUSINESS, 404

        # Check if a review by this user for this business already exists
        stmt = sqlalchemy.text('SELECT id FROM reviews WHERE user_id = :user_id AND business_id = :business_id')
        existing_review = conn.execute(stmt, parameters={'user_id': content['user_id'], 'business_id': content['business_id']}).one_or_none()
        if existing_review is not None:
            return ERROR_CONFLICT_REVIEW, 409

        # Insert the new review into the database
        stmt = sqlalchemy.text(
            'INSERT INTO reviews (user_id, business_id, stars, review_text) '
            'VALUES (:user_id, :business_id, :stars, :review_text)'
        )
        conn.execute(stmt, parameters={
            'user_id': content['user_id'],
            'business_id': content['business_id'],
            'stars': content['stars'],
            'review_text': content.get('review_text', None)
        })
        # Get the last inserted id
        stmt2 = sqlalchemy.text('SELECT LASTVAL()')
        review_id = conn.execute(stmt2).scalar()
        conn.commit()

        # Construct the 'self' URL
        review_url = request.url_root.strip('/') + '/reviews/' + str(review_id)
        business_url = request.url_root.strip('/') + '/businesses/' + str(content['business_id'])

        response_body = {
            'id': review_id,
            'user_id': content['user_id'],
            'business': business_url,
            'stars': content['stars'],
            'review_text': content.get('review_text', None),
            'self': review_url
        }
        return response_body, 201

# Get a review
@app.route('/' + REVIEWS + '/<int:review_id>', methods=['GET'])
def get_review(review_id):
    with db.connect() as conn:
        stmt = sqlalchemy.text(
            'SELECT r.id, r.user_id, r.business_id, r.stars, r.review_text '
            'FROM reviews r WHERE r.id = :review_id'
        )
        row = conn.execute(stmt, parameters={'review_id': review_id}).one_or_none()
        if row is None:
            return ERROR_NOT_FOUND_REVIEW, 404
        else:
            review = row._asdict()
            review['self'] = request.url
            review['business'] = request.url_root.strip('/') + '/businesses/' + str(review['business_id'])
            # Remove 'business_id' from response as per the example
            del review['business_id']
            return review, 200

# Edit a review
@app.route('/' + REVIEWS + '/<int:review_id>', methods=['PUT'])
def put_review(review_id):
    content = request.get_json()

    required_attributes = ['stars']

    # Check if required attribute is missing
    if not content or not all(attr in content for attr in required_attributes):
        return ERROR_MISSING_ATTRIBUTES, 400

    with db.connect() as conn:
        # Check if the review exists
        stmt = sqlalchemy.text('SELECT * FROM reviews WHERE id = :review_id')
        row = conn.execute(stmt, parameters={'review_id': review_id}).one_or_none()

        if row is None:
            return ERROR_NOT_FOUND_REVIEW, 404
        else:
            # Update the review
            stmt = sqlalchemy.text(
                'UPDATE reviews SET '
                'stars = :stars, '
                'review_text = COALESCE(:review_text, review_text) '
                'WHERE id = :review_id'
            )
            conn.execute(stmt, parameters={
                'stars': content['stars'],
                'review_text': content.get('review_text', None),
                'review_id': review_id
            })
            conn.commit()

            # Retrieve the updated review
            stmt = sqlalchemy.text(
                'SELECT r.id, r.user_id, r.business_id, r.stars, r.review_text '
                'FROM reviews r WHERE r.id = :review_id'
            )
            row = conn.execute(stmt, parameters={'review_id': review_id}).one()
            review = row._asdict()
            review['self'] = request.url
            review['business'] = request.url_root.strip('/') + '/businesses/' + str(review['business_id'])
            del review['business_id']
            return review, 200

# Delete a review
@app.route('/' + REVIEWS + '/<int:review_id>', methods=['DELETE'])
def delete_review(review_id):
    with db.connect() as conn:
        stmt = sqlalchemy.text('DELETE FROM reviews WHERE id = :review_id')
        result = conn.execute(stmt, parameters={'review_id': review_id})
        conn.commit()

        if result.rowcount == 1:
            return '', 204
        else:
            return ERROR_NOT_FOUND_REVIEW, 404

# List all reviews for a user
@app.route('/users/<int:user_id>/reviews', methods=['GET'])
def get_user_reviews(user_id):
    with db.connect() as conn:
        stmt = sqlalchemy.text(
            'SELECT r.id, r.user_id, r.business_id, r.stars, r.review_text '
            'FROM reviews r WHERE r.user_id = :user_id'
        )
        rows = conn.execute(stmt, parameters={'user_id': user_id})

        reviews = []
        for row in rows:
            review = row._asdict()
            review['self'] = request.url_root.strip('/') + '/reviews/' + str(review['id'])
            review['business'] = request.url_root.strip('/') + '/businesses/' + str(review['business_id'])
            del review['business_id']
            reviews.append(review)

        return reviews, 200

if __name__ == '__main__':
    init_db()
    create_tables(db)
    app.run(host='0.0.0.0', port=8080, debug=True)










"""


from __future__ import annotations

import logging
import os

from flask import Flask, request

import sqlalchemy

from connect_connector import connect_with_connector

BUSINESSES = 'businesses'
OWNERS = 'owners'
ERROR_NOT_FOUND_BUSINESS = {'Error': 'No business with this business_id exists'}
ERROR_MISSING_ATTRIBUTES = {'Error': 'The request body is missing at least one of the required attributes'}

app = Flask(__name__)

logger = logging.getLogger()

# Sets up connection pool for the app
def init_connection_pool() -> sqlalchemy.engine.base.Engine:
    if os.environ.get('INSTANCE_CONNECTION_NAME'):
        return connect_with_connector()
        
    raise ValueError(
        'Missing database connection type. Please define INSTANCE_CONNECTION_NAME'
    )

# This global variable is declared with a value of `None`
db = None

# Initiates connection to database
def init_db():
    global db
    db = init_connection_pool()

# Create 'businesses' table in the database if it does not already exist
def create_table(db: sqlalchemy.engine.base.Engine) -> None:
    with db.connect() as conn:
        conn.execute(
            sqlalchemy.text(
                'CREATE TABLE IF NOT EXISTS businesses ('
                'id SERIAL NOT NULL, '
                'owner_id INTEGER NOT NULL, '
                'name VARCHAR(50) NOT NULL, '
                'street_address VARCHAR(100) NOT NULL, '
                'city VARCHAR(50) NOT NULL, '
                'state VARCHAR(2) NOT NULL, '
                'zip_code INTEGER NOT NULL, '
                'PRIMARY KEY (id) );'
            )
        )
        conn.commit()

@app.route('/')
def index():
    return 'Please navigate to /businesses to use this API'

# Create a business
@app.route('/' + BUSINESSES, methods=['POST'])
def post_businesses():
    content = request.get_json()

    required_attributes = ['owner_id', 'name', 'street_address', 'city', 'state', 'zip_code']

    # Check if any required attribute is missing
    if not content or not all(attr in content for attr in required_attributes):
        return ERROR_MISSING_ATTRIBUTES, 400

    try:
        with db.connect() as conn:
            # Insert the new business into the database
            stmt = sqlalchemy.text(
                'INSERT INTO businesses (owner_id, name, street_address, city, state, zip_code) '
                'VALUES (:owner_id, :name, :street_address, :city, :state, :zip_code)'
            )
            conn.execute(stmt, parameters={
                'owner_id': content['owner_id'],
                'name': content['name'],
                'street_address': content['street_address'],
                'city': content['city'],
                'state': content['state'],
                'zip_code': content['zip_code']
            })
            # Get the last inserted id
            stmt2 = sqlalchemy.text('SELECT LASTVAL()')
            business_id = conn.execute(stmt2).scalar()
            conn.commit()

            # Construct the 'self' URL
            business_url = request.url_root.strip('/') + '/businesses/' + str(business_id)

            response_body = {
                'id': business_id,
                'owner_id': content['owner_id'],
                'name': content['name'],
                'street_address': content['street_address'],
                'city': content['city'],
                'state': content['state'],
                'zip_code': content['zip_code'],
                'self': business_url
            }
            return response_body, 201
    except Exception as e:
        logger.exception(e)
        return {'Error': 'Unable to create business'}, 500

# Get all businesses with optional pagination
@app.route('/' + BUSINESSES, methods=['GET'])
def get_businesses():
    offset = request.args.get('offset', default=0, type=int)
    limit = request.args.get('limit', default=3, type=int)

    with db.connect() as conn:
        # Fetch total number of businesses
        total_stmt = sqlalchemy.text('SELECT COUNT(*) FROM businesses')
        total = conn.execute(total_stmt).scalar()

        # Fetch businesses with offset and limit
        stmt = sqlalchemy.text(
            'SELECT id, owner_id, name, street_address, city, state, zip_code '
            'FROM businesses ORDER BY id LIMIT :limit OFFSET :offset'
        )
        rows = conn.execute(stmt, parameters={'limit': limit, 'offset': offset})

        businesses = []
        for row in rows:
            business = row._asdict()
            business['self'] = request.url_root.strip('/') + '/businesses/' + str(business['id'])
            businesses.append(business)

        response_body = {'entries': businesses}

        # Calculate next offset
        next_offset = offset + limit
        if next_offset < total:
            next_url = request.url_root.strip('/') + '/businesses?offset={}&limit={}'.format(next_offset, limit)
            response_body['next'] = next_url

        return response_body, 200

# Get a business
@app.route('/' + BUSINESSES + '/<int:business_id>', methods=['GET'])
def get_business(business_id):
    with db.connect() as conn:
        stmt = sqlalchemy.text(
            'SELECT id, owner_id, name, street_address, city, state, zip_code '
            'FROM businesses WHERE id = :business_id'
        )
        row = conn.execute(stmt, parameters={'business_id': business_id}).one_or_none()
        if row is None:
            return ERROR_NOT_FOUND_BUSINESS, 404
        else:
            business = row._asdict()
            business['self'] = request.url
            return business, 200

# Edit a business
@app.route('/' + BUSINESSES + '/<int:business_id>', methods=['PUT'])
def put_business(business_id):
    content = request.get_json()

    required_attributes = ['owner_id', 'name', 'street_address', 'city', 'state', 'zip_code']

    # Check if any required attribute is missing
    if not content or not all(attr in content for attr in required_attributes):
        return ERROR_MISSING_ATTRIBUTES, 400

    with db.connect() as conn:
        # Check if the business exists
        stmt_check = sqlalchemy.text('SELECT id FROM businesses WHERE id = :business_id')
        row = conn.execute(stmt_check, parameters={'business_id': business_id}).one_or_none()

        if row is None:
            return ERROR_NOT_FOUND_BUSINESS, 404
        else:
            # Update the business
            stmt = sqlalchemy.text(
                'UPDATE businesses SET '
                'owner_id = :owner_id, '
                'name = :name, '
                'street_address = :street_address, '
                'city = :city, '
                'state = :state, '
                'zip_code = :zip_code '
                'WHERE id = :business_id'
            )
            conn.execute(stmt, parameters={
                'owner_id': content['owner_id'],
                'name': content['name'],
                'street_address': content['street_address'],
                'city': content['city'],
                'state': content['state'],
                'zip_code': content['zip_code'],
                'business_id': business_id
            })
            conn.commit()

            # Construct the 'self' URL
            business_url = request.url_root.strip('/') + '/businesses/' + str(business_id)

            response_body = {
                'id': business_id,
                'owner_id': content['owner_id'],
                'name': content['name'],
                'street_address': content['street_address'],
                'city': content['city'],
                'state': content['state'],
                'zip_code': content['zip_code'],
                'self': business_url
            }
            return response_body, 200

# Delete a business
@app.route('/' + BUSINESSES + '/<int:business_id>', methods=['DELETE'])
def delete_business(business_id):
    with db.connect() as conn:
        # Optionally delete related reviews here if implemented
        stmt = sqlalchemy.text('DELETE FROM businesses WHERE id = :business_id')
        result = conn.execute(stmt, parameters={'business_id': business_id})
        conn.commit()

        if result.rowcount == 1:
            return '', 204
        else:
            return ERROR_NOT_FOUND_BUSINESS, 404

# List all businesses for an owner
@app.route('/' + OWNERS + '/<int:owner_id>/businesses', methods=['GET'])
def get_owner_businesses(owner_id):
    with db.connect() as conn:
        stmt = sqlalchemy.text(
            'SELECT id, owner_id, name, street_address, city, state, zip_code '
            'FROM businesses WHERE owner_id = :owner_id'
        )
        rows = conn.execute(stmt, parameters={'owner_id': owner_id})

        businesses = []
        for row in rows:
            business = row._asdict()
            business['self'] = request.url_root.strip('/') + '/businesses/' + str(business['id'])
            businesses.append(business)

        return businesses, 200

if __name__ == '__main__':
    init_db()
    create_table(db)
    app.run(host='0.0.0.0', port=8080, debug=True)
"""