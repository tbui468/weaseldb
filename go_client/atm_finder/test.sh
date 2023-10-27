curl http://localhost:8080/atms/0

curl http://localhost:8080/atms --header "Content-Type: application/json" --request "POST" --data '{"bank_name": "test name", "address": "test address", "country": "test country", "city": "test city", "state": "test state", "zip_code": "9999"}'

curl http://localhost:8080/atms/0 --header "Content-Type: application/json" --request "DELETE"

curl http://localhost:8080/atms/1 --header "Content-Type: application/json" --request "PATCH" --data '{"bank_name": "test name", "zip_code": "9999"}'
