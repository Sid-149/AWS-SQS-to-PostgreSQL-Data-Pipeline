# AWS SQS to PostgreSQL Data Pipeline

This project demonstrates a small application that reads JSON data containing user login behavior from an AWS SQS Queue, masks sensitive information, and then writes the transformed data into a PostgreSQL database. It makes use of Python, Boto3 for AWS interactions, psycopg2 for PostgreSQL connections, and Docker for running the necessary components locally.

## Project Setup
To run this project, you'll need the following prerequisites:  
- Docker (https://www.docker.com/get-started)  
- Docker Compose  
- Python 3.x  
- awscli-local (install via pip install awscli-local)  
- PostgreSQL (installed via Docker) (Note: if you have locally installed PostgreSQL, deactivate it before running the application as it may interfere with the one installed via docker)  

## Running the Project
1. Clone this repository to your local machine
2. Make sure you have Docker and Docker Compose installed
3. Start the docker containers for LocalStack and PostgreSQL by running this command in the project directory: ```docker-compose up -d```
4. Wait for the containers to be up and running
5. Run the application: ```python Fetching_SQS_Queue.py```  

The application will read messages from the SQS queue, mask sensitive fields, and write the transformed data into the PostgreSQL database.

## Next Steps
If I had more time to continue developing this project, here are some areas I would focus on:  
1. **Error Handling and Logging:** Enhance error handling mechanisms to handle different types of errors more robustly. Implement proper logging to track errors and events during message processing.
2. **Validation and Data Cleansing:** Add data validation and cleansing steps to ensure the received JSON data is well-formed and clean before processing.
3. **Containerization:** Improve the Docker setup for production readiness, considering proper container orchestration, resource management, and scalability.
4. **Scaling and Performance:** Optimize the data pipeline for large-scale data processing, potentially using AWS services like AWS Lambda for high-throughput scenarios.
5. **Data Encryption and Security:** Implement encryption for sensitive data and ensure secure handling of credentials.
