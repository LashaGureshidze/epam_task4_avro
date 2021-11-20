execute `docker-compose up -d` to run kafka and all related services.

navigate into `/kafka-producer` and build the project: `mvn clean package`

run producer: `com.epam.task4.Producer.main()`

navigate into `/kafka-consumer` and build the project: `mvn clean package`

run consumer: `com.epam.task4.Consumer.main()`

http://localhost:9021/  -- kafka-ui