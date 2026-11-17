import random

# List of 13 unique members
members = ["Ismail+Najam","Zahra","Sadeel Anjum","Sadeel2", "Sadeel Anjum3","Haider Raza"
    ,"Usman Ali","Humera 1", "Humera 2","Zahra", "Hammad","Arsalan", "Aqsa"]

# Ensure the list has exactly 14 members
if len(members) != 13:
    raise ValueError("You need exactly 13 members.")

# Numbers from 1 to 14
numbers = list(range(1, 14))

# Shuffle members to randomize
random.shuffle(members)

# Pair each number with a random member
assignments = list(zip(numbers, members))
# Sort by number (not strictly needed here since numbers are already sorted)
assignments.sort()

# Print the result
for number, member in assignments:
    print(f"Number {number}: {member}")

