import numpy as np
import pandas as pd

#RSEtoInpDat function: prepare data for sankey plot (from RSE/replica to Input-Dataset)
def RSEtoInpDat(TASK):
    print(TASK)
    count_rows = 0
    for k in TASK["data"]["datasets"]:
        for j in TASK["data"]["datasets"][k]["replica"]:
            count_rows = count_rows + 1
    f = count_rows
    c = 3 # c: number of columns
    M =[] #Defining my matrix
    for t in range(f):
        M.append([0]*c)
    count_j = 0
    for k in TASK["data"]["datasets"]:
        for j in TASK["data"]["datasets"][k]["replica"]:
            M[count_j][0] = j
            M[count_j][1] = k
            M[count_j][2] = 1
            count_j = count_j + 1
    return M

#InpDattoSITE function: prepare data for sankey plot (from Input-Dataset to Site)
def InpDattoSITE(TASK):
    count_rows = 0
    for k in TASK["data"]["datasets"]:
        for i in TASK["data"]["datasets"][k]["jobs"]:
            count_rows = count_rows + 1
    f = count_rows
    c = 3 # c: number of columns
    M =[] #Defining my matrix
    for t in range(f):
        M.append([0]*c)
    count_j = 0
    for k in TASK["data"]["datasets"]:
        for j in TASK["data"]["datasets"][k]["jobs"]:
            M[count_j][0] = k
            M[count_j][1] = j
            M[count_j][2] = 1
            count_j = count_j + 1
    return M

#SITEtoJOB function: prepare data for sankey plot (from site to Input-Dataset)
def SITEtoJOB(TASK):
    count_dat = 0 #count over datasets
    for k in TASK["data"]["datasets"]: 
        count_dat = count_dat + 1 
        f = len(TASK["data"]["datasets"][k]["jobs"]) # f: number of rows
        c = 6 # c: number of columns
        M =[] #Defining my matrix
        for t in range(f):
            M.append([0]*c)       
        count_jobs = 0
        for i in TASK["data"]["datasets"][k]["jobs"]:  
            a2 = a3 = a4 = a5 =0
            a1 = i #jobs (site name)
            for j in TASK["data"]["datasets"][k]["jobs"][i]:
                if j == "finished":
                    a2 = (TASK["data"]["datasets"][k]["jobs"][i]["finished"])       
                if j == "failed":
                    a3 = (TASK["data"]["datasets"][k]["jobs"][i]["failed"])      
                if j == "closed":
                    a4 = (TASK["data"]["datasets"][k]["jobs"][i]["closed"])  
                if j == "active":
                    a5 = (TASK["data"]["datasets"][k]["jobs"][i]["active"])   
            M[count_jobs][0]=k      #dataset name
            M[count_jobs][1]=a1     #jobs (site)
            M[count_jobs][2]=a2     #finished
            M[count_jobs][3]=a3     #failed
            M[count_jobs][4]=a4     #closed
            M[count_jobs][5]=a5     #active
            count_jobs = count_jobs + 1
        if count_dat == 1:
            jobs = [[M[0][1],"finished",M[0][2]],
                    [M[0][1],"failed",M[0][3]],
                    [M[0][1],"closed",M[0][4]],
                    [M[0][1],"active",M[0][5]]]           
            for x in range(1, f, 1):
                a1 =[M[x][1],"finished",M[x][2]]
                jobs = np.vstack([jobs, a1])       
                a2 =[M[x][1],"failed",M[x][3]]
                jobs = np.vstack([jobs, a2])      
                a3 =[M[x][1],"closed",M[x][4]]
                jobs = np.vstack([jobs, a3])   
                a4 =[M[x][1],"active",M[x][5]]
                jobs = np.vstack([jobs, a4])
        if count_dat > 1:
            for x in range(0, f, 1):
                a1 =[M[x][1],"finished",M[x][2]]
                jobs = np.vstack([jobs, a1])          
                a2 =[M[x][1],"failed",M[x][3]]
                jobs = np.vstack([jobs, a2])         
                a3 =[M[x][1],"closed",M[x][4]]
                jobs = np.vstack([jobs, a3])     
                a4 =[M[x][1],"active",M[x][5]]
                jobs = np.vstack([jobs, a4])
    count_s = 0
    for s in range(len(jobs)):
        if jobs[s][2] != "0":
            count_s = count_s + 1
    ff = count_s
    cc = 3 # c: number of columns
    MM =[] #Defining my matrix
    for t in range(ff):
        MM.append([0]*cc)
    count_i = 0
    for i in range(len(jobs)):
        if jobs[i][2] != "0":
            MM[count_i][0] = jobs[i][0]
            MM[count_i][1] = jobs[i][1]
            MM[count_i][2] = int(jobs[i][2])
            count_i = count_i + 1
    return MM

#frec function: calculate the frcuencies in a proper way for sankey plot
def frec(M):
    A = np.array(M)
    df = pd.DataFrame(A, columns=['X', 'Y', 'frec'])
#    print(df)
    df['frec'] = df['frec'].apply(int)
    df = df.groupby(['X','Y'], as_index=False)['frec'].sum()
    NP = df.to_numpy() #convert from panda-dataframe to numpy-array
    b = NP.tolist() #convert from numpy-array to a list
    return b

#concat function: join the data in a proper format for sankey plot.
def concat(TASK):
    A = pd.DataFrame(frec(RSEtoInpDat(TASK)))
    B = pd.DataFrame(frec(InpDattoSITE(TASK)))
    C = pd.DataFrame(frec(SITEtoJOB(TASK)))
    D = pd.concat([A, B, C], ignore_index=True)
    E = D.to_numpy()
    return E.tolist()








