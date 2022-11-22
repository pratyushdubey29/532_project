This Repo consist of code and documentation needed for successfully running the project End to End.
Below are the steps needed to be installed before running this project : 

1) Install Spark / PySpark: (Assuming Mac OS)

# Install Homebrew
    /bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"

# Set brew to Path
    echo 'eval "$(/opt/homebrew/bin/brew shellenv)"' >> /Users/admin/.zprofile
    eval "$(/opt/homebrew/bin/brew shellenv)"

# Install OpenJDK 11
    brew install openjdk@11

# Install Scala (optional)
    brew install scala

# install Python
    brew install python


# Install Apache Spark
    brew install apache-spark


2) Install machine learning libraries (Assuming already configured anaconda installed)

# Install Pandas 
    conda install pandas

# Install Scikit-learn 
    pip install -U scikit-learn

# Install pytorch 
    Please refer website : https://pytorch.org/get-started/locally/

# Install MySQL
    https://dev.mysql.com/downloads/installer/
    
#Install MySQL Connector
    
    https://jar-download.com/artifacts/mysql/mysql-connector-java/5.1.48/source-code



 
