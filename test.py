test = [4004,4020,4036,4052,4068,"x"]
for i in range(len(test)-1):
    j = i+1
    test[i] +=9
    if test[j] == "x":
        break
    diff = test[j]-test[i]
    print(diff)
print(test)

let's build this playbook hello ( id int, line text,succes_rate int);
slam this into hello ( id , line ) this crazy shit (0 , "might workd now");
show me all from hello;