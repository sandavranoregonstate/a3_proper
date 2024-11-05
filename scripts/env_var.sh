#!/bin/bash

# Move up one directory
cd ..

# Set environment variables for database connection
INSTANCE_CONNECTION_NAME='cs-493-a3-440723:us-central1:a3-db'
DB_NAME='a3' 
DB_USER='a3-user-try'
DB_PASS='0000'
GOOGLE_APPLICATION_CREDENTIALS='./a3-key.json'

# Export the variables so they are available to child processes
export INSTANCE_CONNECTION_NAME
export DB_NAME
export DB_USER 
export DB_PASS
export GOOGLE_APPLICATION_CREDENTIALS