module lb_sim

#originally implemented in python, but don't have a copy of it unfortauntely.
#Juia implementation had significant speed boosts

#Fyle type definition
#A Fyle has a size which will decrement each time step
#origSize is the inital size of the Fyle

type Fyle
        size::Float64
        origSize::Float64
        lifetime::Int


        #get_lifetime::Function
        #get_size::Function
        #function Fyle(size = 0)
        #        this = new()

        #        this.size=size

        #        this.lifetime=0
        #        this.origSize=size

#Removed having these as function calls. Will
        #this.get_lifetime = function()
        #        return(this.lifetime)
        #end
        #this.get_size = function()
        #    return (this.size)
        #end
    #return this
    #end
end

#outer constructor
Fyle(size=0) = Fyle(size, size, 0)


type Node
    threads::Vector{Fyle}
    maxthreads::Int
    complete::Vector{Fyle}
    percent::Float64
    name::UTF8String
end

#Outer Constructor
#Node(maxthreads=400;name="Default") = Node(name)
Node() = Node(maxthreads=400;name="Default")
Node(given_name) = Node(Fyle[], 400, Fyle[], 0.0, given_name)

#Begin moving the functions which we previously defined within Node to be external functions which can only be called on Node objects
function add_file(n::Node, f)
        push!(n.threads, f)
end

function pop_file(n::Node)
        return pop!(n.threads)
end

function process_threads(n::Node)
        for t in n.threads
            if t.size<=0
               push!(n.complete,t)
               i = findIndex(n, n.threads,t)
               splice!(n.threads, i)
            else t.size = t.size - 0.025
               t.lifetime = t.lifetime + 1
           end
        end
end

function findIndex(n::Node, threads::Array{Fyle}, t::Fyle)
        for i in 1:length(n.threads)
             if n.threads[i] == t
                    return i
             end
        end
end

function avg_lifetime(n::Node)
        tot=0
        for f in n.complete
           tot+=f.lifetime
        end
        if length(n.complete)==0
           return -1
        end

        ans = tot/length(n.complete)
           return ans
end

function thread_utilization(n::Node)
    n.percent=(length(n.threads)/n.maxthreads)*100
    return n.percent
end

#not adding the to_string method here

function num_files(t::Int)
        num =  (111.659178 + 7.36795 *  sin(2* (pi/86400)* t) +  59.775973 * cos(2 * (pi/86400) * t)
        + 10.015332 * sin(4 * (pi/86400)* t) + (-6.521536)  * cos(4*(pi/86400)*t) + randn()*(14.47911) )# + noise term to be decided

        return(num)
end


function get_filesize()
        filesize=inverse_transform_sample()
    return(filesize)

end


function inverse_transform_sample()
        u = rand()
        probs = [0,0.523659111820799,0.603700554729777,0.768642697077195,0.837174082853255,0.941976988781107,0.947192060243916,0.952337001218746,0.953269824074137,0.971778157331356,0.972342298731258,0.986229406118667,0.988138178505522,0.988167962121594,0.988197110353857,0.989209435608418,0.989227782315918,0.989259630929372,0.989318483354731,0.989533799043191,0.989580579176169,0.989606153374504,0.989747049734271,0.989762219522723,0.989777389311176,0.989783425457367,0.989900892039157,0.989909946258443,0.989958870811778,0.989961491769993,0.989966892532374,0.989987860198089,0.989991751923922,0.990010336900352,0.990011925359875,0.990016452469518,0.990066012406663,0.990067918558092,0.990070380670354,0.990071651437973,0.990081499887021,0.990082691231664,0.990094922369998,0.990095398907855,0.990116922534403,0.990117319649284,0.990117955033094,0.990123435218451,0.990123752910356,0.990128756557856,0.990129391941665,0.990129630210594,0.990129947902499,0.993298289268804,0.993298448114757,0.993298527537733,0.993301307341900,0.993301545610828,0.993301704456781,0.993303213493328,0.993309567331424,0.993312108866662,0.993312188289638,0.993313538480233,0.993313617903210,0.993314888670829,0.993322354430591,0.993324101736067,0.993324181159043,0.993325213657734,0.993325372503686,0.993325451926662,0.993326722694281,0.993332520571544,0.995424918879358,0.995425236571262,0.995425554263167,0.995430796179596,0.995431828678287,0.995432464062096,0.995432622908048,0.995433337714834,0.995436594056858,0.995436991171739,0.995437547132572,0.995437864824477,0.996407381094870,0.996408969554393,0.996409604938203,0.996410558013917,0.996411431666655,0.996411749358560,0.996412384742370,0.996413178972132,0.996413417241060,0.996413893778917,0.996414767431656,0.997010439753106,0.997010519176082,0.997010757445011,0.997011948789653,0.997012107635606,0.997012345904534,0.997012981288344,0.997013060711320,0.997013616672153,0.997014172632987,0.997014410901915,0.997571324810983,0.997571801348840,0.997572675001579,0.997572992693483,0.997573628077293,0.997573707500269,0.997573866346221,0.997573945769198,0.997574025192174,0.997937226462306,0.997937385308258,0.997938417806949,0.997938576652901,0.997938656075878,0.997939132613735,0.997939212036711,0.997939450305639,0.997939767997544,0.997940165112425,0.998231329743150,0.998231488589102,0.998232123972912,0.998232282818864,0.998232521087793,0.998232600510769,0.998232679933745,0.998232838779698,0.998233235894579,0.998233315317555,0.998545844728876,0.998545924151852,0.998546241843757,0.998546321266733,0.998546638958638,0.998546797804590,0.998546877227566,0.998546956650542,0.998547115496495,0.998547194919471,0.998779348278884,0.998779586547813,0.998779904239718,0.998779983662694,0.998780142508646,0.998780221931622,0.998935573273057,0.998935732119009,0.998935811541985,0.998935890964961,0.998935970387938,0.998936208656866,0.998936367502819,0.998936446925795,0.999072021946157,0.999072101369133,0.999072180792109,0.999072419061038,0.999072577906990,0.999072657329966,0.999072975021871,0.999073054444847,0.999073292713776,0.999073372136752,0.999269864579854,0.999269944002831,0.999270341117712,0.999270420540688,0.999270499963664,0.999270579386640,0.999375258869263,0.999375338292239,0.999375497138192,0.999375735407120,0.999376053099025,0.999462782989028,0.999521476568435,0.999521635414387,0.999521714837364,0.999521794260340,0.999521873683316,0.999521953106292,0.999590415711771,0.999590495134747,0.999590574557723,0.999590653980699,0.999635289693320,0.999635369116296,0.999635448539272,0.999671427147488,0.999671506570464,0.999671585993440,0.999705737873204,0.999705817296180,0.999732185724276,0.999732265147252,0.999750294162848,0.999750373585824,0.999750453008800,0.999771579520468,0.999771897212373,0.999771976635349,0.999788973152254,0.999789131998207,0.999805175439398,0.999805254862374,0.999819948112970,0.999820027535946,0.999834561940589,0.999834641363565,0.999834720786542,0.999851479034518,0.999851558457495,0.999851637880471,0.999868237282495,0.999868316705471,0.999879753614043,0.999879833037019,0.999879991882972,0.999887537065710,0.999887695911663,0.999895082248449,0.999895161671425,0.999895320517377,0.999895399940353,0.999903262814997,0.999903898198806,0.999903977621782,0.999911999342378,0.999920100485949,0.999927645668688,0.999927725091664,0.999934476044640,0.999934555467617,0.999938129501545,0.999942736034165,0.999942815457141,0.999948772180355,0.999951631407498,0.999954093519760,0.999956952746903,0.999957032169879,0.999960685626784,0.999963941968808,0.999964100814761,0.999966404081070,0.999967674848689,0.999970375229880,0.999970454652856,0.999970534075832,0.999970613498808,0.999972281381309,0.999972360804285,0.999974028686785,0.999974108109761,0.999974187532737,0.999974266955713,0.999976331953094,0.999976411376071,0.999977920412618,0.999977999835594,0.999979350026190,0.999979429449166,0.999979508872142,0.999980064832975,0.999980144255952,0.999980859062737,0.999980938485714,0.999981812138452,0.999982844637142,0.999984115404761,0.999985703864285,0.999985783287261,0.999986656940000,0.999986895208928,0.999987768861666,0.999988483668452,0.999989039629285,0.999989119052262,0.999989198475238,0.999989754436071,0.999990707511785,0.999991184049642,0.999991740010476,0.999991898856428,0.999992375394285,0.999992693086190,0.999992772509166,0.999993169624047,0.999993805007857,0.999993884430833,0.999993963853809,0.999994043276786,0.999994519814643,0.999994758083571,0.999994996352500,0.999995234621428,0.999995314044405,0.999995393467381,0.999995949428214,0.999996187697143,0.999996346543095,0.999996743657976,0.999996823080952,0.999996981926905,0.999997061349881,0.999997140772857,0.999997220195833,0.999997379041786,0.999997458464762,0.999997855579643,0.999997935002619,0.999998173271548,0.999998411540476,0.999998570386428,0.999998649809405,0.999998888078333,0.999999126347262,0.999999841154048,0.999999920577024,1.000000000000000]

        vals = [0,0.5,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,49,50,51,52,54,55,56,57,58,59,61,62,63,65,67,69,70,72,74,75,76,77,79,82,85,88,92,94,96,100,102,103,106,109,112,115,117,118,119,124,127,128,133,134,137,139,142,143,144,148,150,152,155,158,159,164,166,167,168,170,175,179,182,185,188,190,193,195,197,199,202,205,208,213,216,219,220,221,225,230,237,238,241,244,248,249,250,251,256,260,263,269,272,274,279,281,284,285,290,291,299,301,303,314,316,320,325,326,330,333,341,342,343,352,353,355,363,365,370,375,376,381,382,398,403,411,416,422,423,439,442,453,456,488,489,492,498,506,517,523,544,545,555,557,559,574,585,604,613,617,626,649,650,675,680,687,700,701,711,727,746,754,766,783,808,811,820,836,845,849,883,886,913,928,936,954,963,966,992,999,1006,1014,1022,1024,1048,1054,1091,1134,1147,1183,1189,1220,1248,1270,1293,1338,1368,1397,1427,1430,1469,1495,1509,1541,1579,1591,1601,1610,1616,1641,1656,1660,1674,1684,1688,1706,1719,1727,1744,1757,1769,1781,1792,1810,1824,1848,1884,1938,1993,2007,2041,2048,2078,2118,2148,2163,2191,2193,2232,2274,2324,2350,2412,2446,2485,2510,2556,2599,2651,2658,2699,2733,2776,2808,2837,2873,2908,2950,3015,3037,3065,3090,3116,3141,3193,3224,3240,3337,3426,3471,3594,3642,3704,3841,3892,4094,4144,5593]
        return(vals[bin_search(probs,u)])
end


function bin_search(ar, key)
#a sloppy binary search
#does not work for a fairly common edge case, so I altered the original arrays. Will eventually fix this (date:2/10/2015)
#Note to self: Fix edge case of key <0.5 and return array index 1
        if(length(ar)==0)
                return "Key Not Found"
        end

        high = length(ar)
        low = 1

        while(high > low)
                index = round((high + low)/2)
                #print("index")
                #print(index)
                #print("\n")
                sub = ar[index]
                if(ar[low]==key)
                        return low
                        elseif(ar[high]==key)
                                return high
                end
                if(sub > key)
                        if(high==index)
                                return(high)
                        end
                   high = index
                 else
                      if(low==index)
                              return(low)
                      end
                      low=index
                end
         end
       return("Whats going on here....")
end

function find_min_threads(cluster::Array{Node})
        mini = cluster[1]
        #println("find_min called")
        for n in cluster
                #println(n.name)
                if mini==n
                        continue
                elseif thread_utilization(mini) > thread_utilization(n)
                        mini = n
                        continue
                end
        end
   return(mini)
end

function greedy_algo(incoming_files::Array{Fyle}, cluster::Array{Node})

        for f in 1:length(incoming_files)
                #println("IS THIS LOOP RUNNING")
                node=find_min_threads(cluster)
                add_file(node,incoming_files[f])
        end
        #println("Greedy finished")
        return(cluster)
end


#takes in an array of incoming files and distributes them based on round robin algorithm, takes in the last index so it knows where to begin next
#time step
function round_robin(incoming_files::Array{Fyle}, cluster::Array{Node}, last_index::Int)
        index = last_index
        rollover = length(cluster)

        for i in 1:length(incoming_files)
                temp = incoming_files[i]
                add_file(cluster[index],temp)
                index = index + 1

                if index > rollover
                        index=1
                end
        end
        return(cluster, index)
end

function minute_avg(dataArray::Array{Float64, 2}, nodes::Int64)
        ans = Float64[]
        #println(nodes)
        for i in 1:nodes
                #println(i)
                tmp = sum(dataArray[:,i])/length(dataArray[:,i])
                push!(ans, tmp)
        end
        #println("line 207")
        return(ans)
end


function minute_aggregate(minDataRow)
        avg = sum(minDataRow)/length(minDataRow)
        return(avg)
end







#computes how much data each Node has computed fully. Does not count files currently being processed by threads.
function get_file_sum(node::Node)
        totes = 0
        for c in node.complete
                totes = totes + c.origSize
        end
        return(totes)
end

#the Above code is only the function and class (type) definitions.
#Below begins the actual running code.





function run(algo::String, path1::String, path2::String, path3::String, path4::String, path5::String, seed::Int64, sim_time::Int64, time_file)
        #Creates the MersejjeTwister object for random number generation.


        print("Running $algo...\n")

        #Cluster represented as an Array of node Type.
        cluster=Node[]
        #Reporting Rows
        datarows = Float64[]
        liferows = Float64[]

        num_nodes = 100

        #I am able to use Julia's native Array/Matrix support to replace the fucntionality once given by Python's Numpy Package
        minuteData = Array(Float64, 60, num_nodes)
        minuteLife = Array(Float64, 60, num_nodes)

        minDataRow = Float64[]
        minLiferow = Float64[]
        tempAgg = Float64[]

        #While the Julia Docs (@julialang.org) seem to imply a range() function exists, I'm unable to find it
        #Thus, I have used the : syntax to display this, sim_time is still an iterable object.

        #removed sim_time delcaration, will now be passed as an argument to the run() function
        rr_index = 1


        #Output is written to .csv files
        #At current writing Python's: 'wb+' seems to be Julia's 'r+' -- actually was the "w" option
        #Excluding the 'with' statement form python code, was unneccessary in julias implmentation.
        datafile = open(path1, "w")
        lifer = open(path2, "w")
        minLife = open(path3, "w")
        minThread = open(path4, "w")
        aggThread = open(path5, "w")

        #Will not have to create csv.writer objects in Julia, intsead I will be using the writedlm(filename, iterable, delim) function
        #on my above files then close them out in the end. Similar to Pythons implmentation in most ways.
        #since I use a function directly on my files, without having to create Python's csv.writer objects this will clean up the code a bit.
        #Statements like minThread_scrib.writerow(minLifeRow) in Python will become writedlm(minThread, minDataRow, delim=',') in Julia.

        #Creates the cluster Array, instead of Python's .append() function we are using Julia's push!() function to push a node typed object into the Array.
        #Node Names will be interpolated string of the int 'i'

        for i in 1:num_nodes
                #println("Hi")
                push!(cluster, Node("$i"))
        end

        #This for loop, for t in sim_time, contains the entire runtime of the progam. This is what sets t for the simulaition to then run. all objects created
        #and functions called will be kick off in this loop. This will most likely be called from a main 'function' attached to a separate function which will only
        #ask for say, random seed, and number of run simulations you wish to run. Perhaps if num_runs >1 it will expect a iterable of random seeds for the subseququant runs.

        for t in 1:sim_time
                #println(t)
                if (t%1000)==0
                        println("Still Running....  ($algo)")
                end
                fileq = time_file[t]

                if(algo == "rr")
                        #Syntax is identical to Python's for tuple returns.
                        cluster,rr_index = round_robin(fileq,cluster,rr_index)
                else
                        #println("Hello, Hello, Hello, is there anybody in here")
                        cluster = greedy_algo(fileq, cluster)
                end

                for n in cluster


                        push!(datarows,thread_utilization(n))
                        push!(liferows, avg_lifetime(n))

                        #Processes the threads for that time_step
                        process_threads(n)
                end

                #This following section of code uses the modulou operator '%' to determine if a minute has passed
                #of simulated time. This is to get minutely averages since seconds is far too granular for this analysis.



                if (t%60)==0 && ( t != 0)
                        #println("line334")
                        minDataRow = minute_avg(minuteData, num_nodes)
                        #println("line 336")
                        #calculates aggreggate avg to compare algorithm performance

                        #push!(tempAgg, minute_aggregate(minDataRow))

                        tempAgg = minute_aggregate(minDataRow)
                        #println("line 342")

                        minLifeRow = minute_avg(minuteLife, num_nodes)
                        #println("line 345")
                        writecsv(minLife, minLifeRow')
                        #println("line 347")
                        #Recall this change, instead of calling the csv.writeObject we would have created. We use the writedlm Function on the minLife file we opened.
                        writecsv(minThread,minDataRow')
                        #println("line 350")
                        writecsv(aggThread, tempAgg)
                        #println("line 352")
                        #Empties all the data strucutes
                        minDataRow =Float64[]
                        minLifeRow =Float64[]
                        tempAgg = Float64[]
                        minuteData = Array(Float64, 60, num_nodes)
                        minuteLife = Array(Float64, 60, num_nodes)
                end

                minuteData[((t%60) + 1),:] = datarows'
                minuteLife[((t%60)+ 1),:] = liferows'

                writecsv(datafile, datarows')
                writecsv(lifer, liferows')
                datarows = Float64[]
                liferows = Float64[]




        end #End of time loop, at this point the simulation has finished for that algorithm choice

        #close(datafile) replaces datafile.close(), minor difference
        close(datafile)
        close(lifer)
        close(minLife)
        close(minThread)
        close(aggThread)
        #It may be possible to further clean up this implementation. For example store the file paths in an array and iterator through them to
        #open and close the files would be far more elegant, and easier to add/remove file. Might be worth it to implement in future versions



end #end of run()


#Currently unused code
function sendmail()
    using SMTPClient
    #blantantly copied from https://github.com/JuliaWeb/SMTPClient.jl
    SMTPClient.init()
    o=SendOptions(blocking=true, isSSL=true, username="", passwd="")
    #Provide the message body as RFC5322 within an IO
    body=IOBuffer("Date: ??\nFrom: You <you@gmail.com>\nTo: me@test.com\nSubject: Julia Test\n\nSimulation Completed")
    resp=send("outgoing.sncrcorp.net", ["<julia_sim@synchronoss.com>"], "Michael.Bullman@synchronoss.com>", body, o)
    SMTPClient.cleanup()
end


function create_input_files(seed::Int64, sim_time)
  srand(seed)

    time_file = Array{Fyle}[]

    println("Initializing Input Files...")
    for t in 1:sim_time
            #Creates the number of files to be added to the file queue.
            #These will then be added to the dv_nodes based on whichever algorithm is currently running.
            file_count = round(num_files(t),0)
            fileq = Fyle[]



            #If we wanted to be even more effcient with code, we could have directly called 'for f in 1:(Int64(num_file(t))
            #This is just how I prefer to write it out.
            for f in 1:file_count

                push!(fileq,Fyle(get_filesize()))

                #I did use the exact syntax I just said I didn't perfer. However, get_filesize() is much cleaner than inverse_transform_sample() ;)
            end

            push!(time_file, fileq)
   end

   println("Input Files Complete.")
  return(time_file)
end

function equal_fyles(fyle1::Fyle, fyle2::Fyle)
    if(fyle1.size == fyle2.size &&  fyle1.origSize == fyle2.origSize && fyle1.lifetime == fyle2.lifetime )
      return(true)
  else return(false)
  end
end


function main(seed::Int64, sim_time::Int64, islinux::Int)

   lb1 = "rr"
   lb2 = "minimum"


   #Creating file paths"

   #Main file path: The idea is to have the sub paths easily modifiable to add and remove output files.
   if islinux == 1
            paath = "/home/mbul0001/Julia_Sim"
            output = open("$paath/output.txt", "w")
   else
            paath = "C:\\Users\\mbul0001\\Documents\\Analysis\\Sim_Data\\test"
            output = open("$paath\\output.txt", "w")
   end


   write(output,strftime(time())) #Writes the start time of the program. This should give accurate run time stats
   write(output, "\n")

   if islinux ==1
           path1 = "$paath/threadUtilization_$lb1.csv"
           path2 = "$paath/avg_lifetime_$lb1.csv"
           path3 = "$paath/minuteLifetime_$lb1.csv"
           path4 = "$paath/minuteThreads_$lb1.csv"
           path5 = "$paath/aggregate_Threads_$lb1.csv"
           else
                   path1 = "$paath\\threadUtilization_$lb1.csv"
                   path2 = "$paath\\avg_lifetime_$lb1.csv"
                   path3 = "$paath\\minuteLifetime_$lb1.csv"
                   path4 = "$paath\\minuteThreads_$lb1.csv"
                   path5 = "$paath\\aggregate_Threads_$lb1.csv"
   end

   #Runs simulation, returns grand total of simulated ingest
    tf1 = create_input_files(seed, sim_time)
    tf2 = create_input_files(seed, sim_time)
    for i in 1:length(tf1)
      subray1 = tf1[i]
      subray2 = tf2[i]
      if length(subray1) != length(subray2)
        println("diff number of file in sub arrays")
      end
      for j in 1:length(subray1)
        if equal_fyles(subray1[j], subray2[j])
      else println("Hey Whats up with that")
      end
    end


    end

   run(lb1, path1, path2, path3, path4, path5, seed, sim_time, tf1)

   if islinux==1
               path1 = "$paath/ThreadUtilization_$lb2.csv"
               path2 = "$paath/avg_lifetime_$lb2.csv"
               path3 = "$paath/minuteLifetime_$lb2.csv"
               path4 = "$paath/minuteThreads_$lb2.csv"
               path5 = "$paath/aggregate_Threads_$lb2.csv"
               else
                       path1 = "$paath\\ThreadUtilization_$lb2.csv"
                       path2 = "$paath\\avg_lifetime_$lb2.csv"
                       path3 = "$paath\\minuteLifetime_$lb2.csv"
                       path4 = "$paath\\minuteThreads_$lb2.csv"
                       path5 = "$paath\\aggregate_Threads_$lb2.csv"
    end
   #Runs simulation, returns grand total of simulated ingest for second load-balancing(lb) algorithm


   run(lb2, path1, path2, path3, path4, path5, seed, sim_time, tf2)

   println("Simulations Complete.")

   println("Summing files....")
   file_sum = 0
   for t in tf1
        for f in t
                file_sum = file_sum + f.origSize
        end
   end
   file_sum_tb = file_sum/1024/1024 #forgot it was already in MB not Bytes
   write(output, "\n")
   write(output, "$file_sum_tb")
   write(output, "\n")
   write(output,strftime(time()))
   close(output)
   print("Done.")



end




end #ends the module definition
