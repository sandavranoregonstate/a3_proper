from flask import Flask, request, jsonify
from google.cloud import datastore

app = Flask(__name__)
client = datastore.Client()

@app.route('/businesses', methods=['POST'])
def create_business():
    required_fields = ["owner_id", "name", "street_address", "city", "state", "zip_code"]
    request_data = request.get_json()

    # Check if all required fields are present in the request
    if not all(field in request_data for field in required_fields):
        return jsonify({"Error": "The request body is missing at least one of the required attributes"}), 400

    # Create a new entity in the Datastore for the business
    business_entity = datastore.Entity(client.key('Business'))
    business_entity.update({
        "owner_id": request_data["owner_id"],
        "name": request_data["name"],
        "street_address": request_data["street_address"],
        "city": request_data["city"],
        "state": request_data["state"],
        "zip_code": request_data["zip_code"]
    })

    # Save the entity to Datastore
    client.put(business_entity)

    # Prepare response with Datastore generated ID
    response_data = {
        "id": business_entity.key.id,
        "owner_id": request_data["owner_id"],
        "name": request_data["name"],
        "street_address": request_data["street_address"],
        "city": request_data["city"],
        "state": request_data["state"],
        "zip_code": request_data["zip_code"]
    }

    return jsonify(response_data), 201

if __name__ == '__main__':
    app.run(host='127.0.0.1', port=8080, debug=True)
