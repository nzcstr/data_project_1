# Use the last oficial image of Spark. Equal to "Dockerfile.dev3"
FROM bitnami/spark:3.5.5

# Install python pip  and "ping" command
USER root
RUN apt-get update && apt-get install -y python3-pip && \
    pip3 install --upgrade pip && \
    apt-get install -y iputils-ping

# Define environmental variables necessary for Spark
ENV SPARK_HOME=/opt/bitnami/spark
ENV PATH="$SPARK_HOME/bin:$SPARK_HOME/sbin:$PATH"

# Create a working folder to store all code.
WORKDIR /app

# Expose ports used by Spark
EXPOSE 4040 7077 8080

# Copy the requirements file from repository into working directory
COPY requirements.txt .

# Install all packages python packages specified in "requirements.txt"
RUN pip3 install -r requirements.txt

# Define default command when executing container.
CMD ["/bin/bash"]



