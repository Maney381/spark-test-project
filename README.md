# Spark Test Project

This project demonstrates the use of Apache Spark for solving various exercises related to data processing and analysis. The project is written in Scala and uses SBT as the build tool.

## **Setup and Usage**

### Prerequisites
- **Apache Spark** installed locally or in a distributed cluster.
- **Java 8 or higher**.
- **SBT (Scala Build Tool)** installed.

### Steps to Run the Project
1. Clone the repository:
   ```bash
   git clone <repository-url>
   cd spark-test-project
   ```
2. Build the project:
   ```bash
   sbt compile
   ```
3. Run the exercises:
   - **Exercise 1**:
     ```bash
     sbt runMain Exercise1
     ```
   - **Exercise 2**:
     ```bash
     sbt runMain Exercise2
     ```

## **Utilities**

### `SparkSessionProvider`
A utility object to simplify the creation and configuration of a `SparkSession`.
