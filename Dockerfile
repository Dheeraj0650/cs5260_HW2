FROM openjdk:11
COPY producer.jar producer.jar
CMD ["java", "-jar", "producer.jar", "--request-queue=https://sqs.us-east-1.amazonaws.com/174299131857/cs5260-requests"]