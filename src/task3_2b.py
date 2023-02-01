import numpy as np
import pandas as pd
from sklearn.linear_model import LogisticRegression

df = pd.read_csv("iris.csv", 
      names = ["sepal_length", "sepal_width", "petal_length", "petal_width", "class"])
df.head()

# Separate features from class.
array = df.values
X = array[:,0:4]
y = array[:,4]

# Fit Logistic Regression classifier.
logreg = LogisticRegression(C=1e5)
logreg.fit(X, y)

print(logreg.predict([[5.1, 3.5, 1.4, 0.2]]))
print(logreg.predict([[6.2, 3.4, 5.4, 2.3]]))
