text_file = open("./ngrams-output.txt").read().splitlines()
for i in range(0,len(text_file)):
    text_file[i] = eval(text_file[i])

import matplotlib.pyplot as plt
plt.scatter(*zip(*text_file), s= 2)
plt.title('Average Word Length Changes Over Time')
plt.xlabel('Year')
plt.ylabel('Average Word Length')
plt.show()

