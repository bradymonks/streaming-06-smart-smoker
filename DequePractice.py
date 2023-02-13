from collections import deque
import csv

# Create the deques for each queue
FoodAdeque = deque(maxlen=20)
FoodBdeque = deque(maxlen=20)
SmokerDeque = deque(maxlen=5)

# Threshold for smoker temperature change
smoker_threshold = 15

# Threshold for food temperature change
food_threshold = 1

# read from a file to get some temperature data
with open("smoker-temps.csv", "r") as input_file:
    tasks = (input_file)

    # create a csv reader for our comma delimited data
    reader = csv.reader(tasks, delimiter=",")

    # Skip the first row which contains column names
    next(reader)

    for row in reader:
        # read the second, third, and fourth columns of data
        column2 = row[1] if row[1] else None
        column3 = row[2] if row[2] else None
        column4 = row[3] if row[3] else None

        if column2:
            # Check if the temperature in the SmokerDeque has decreased by more than 15 F
            if len(SmokerDeque) >= 5:
                prev_temp = SmokerDeque[-5]
                temp_diff = float(prev_temp) - float(column2)
                if temp_diff >= smoker_threshold:
                    print("ALERT: The temperature in the smoker has decreased by more than 15 F in 2.5 min")

            # append the data from the second column to the SmokerDeque
            SmokerDeque.append(column2)

        if column3:
            # Check if the temperature in the FoodAdeque has changed by 1 F or less in 10 min
            if len(FoodAdeque) >= 20:
                prev_temp = FoodAdeque[-20]
                temp_diff = abs(float(column3) - float(prev_temp))
                if temp_diff <= food_threshold:
                    print("ALERT: The temperature in Food A has changed by 1 F or less in 10 min")

            # append the data from the third column to the FoodAdeque
            FoodAdeque.append(column3)

        if column4:
            # Check if the temperature in the FoodBdeque has changed by 1 F or less in 10 min
            if len(FoodBdeque) >= 20:
                prev_temp = FoodBdeque[-20]
                temp_diff = abs(float(column4) - float(prev_temp))
                if temp_diff <= food_threshold:
                    print("ALERT: The temperature in Food B has changed by 1 F or less in 10 min")

            # append the data from the fourth column to the FoodBdeque
            FoodBdeque.append(column4)
