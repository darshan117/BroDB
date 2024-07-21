test = [4004,4020,4036,4052,4068,"x"]
for i in range(len(test)-1):
    j = i+1
    test[i] +=9
    if test[j] == "x":
        break
    diff = test[j]-test[i]
    print(diff)
print(test)

