import random

product_counts = {'A': 0, 'B': 0, 'C': 0, 'D': 0, 'E': 0}

with open('input.txt', 'w') as the_file:
    for i in range(1, 101):
        purchase = []
        products = ['A', 'B', 'C', 'D', 'E']
        r = random.randint(2, 5)
        for x in range(r):
            prod = random.choice(products)
            purchase.append(prod)
            products.remove(prod)
        line = " ".join(purchase)
        the_file.write(f"{i}, {line}\n")