from flask import Flask, request, jsonify
from google.cloud import datastore

app = Flask(__name__)
datastore_client = datastore.Client()

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

if __name__ == '__main__':
    app.run(host='localhost', port=8080, debug=True)
