#r "nuget: Akka.FSharp" 
#r "nuget: Akka.TestKit"

open System
open Akka.FSharp
open Akka.Actor
open System.Threading
open Akka.Configuration

let system = System.create "MySystem" <| Configuration.load()
let mutable numNodes = 0
let mutable numRequests = 0
let mutable numDigits = 0
let mutable node = ""
let mutable hexDecNum = ""
let mutable hexDeclen = 0
let mutable actorMap = Map.empty
let mutable actorHopsMap:Map<string,double array> = Map.empty
let mutable avgHopSize = 0.0
let mutable flag = false
type ProcessorJob = 
    |Msg of int * int
    |Build of string * int
    |Join of string * int
    |Update of string array
    |Routing of string * string * int

let workerref (mailbox : Actor<_>) =
        let mutable id = ""
        let mutable rows = 0
        let cols = 16
        let mutable itr,number,left,right,currentRow,rtrow,rtcol = 0,0,0,0,0,0,0
        let mutable routingTable: string[,] = Array2D.zeroCreate 0 0
        let mutable leafSet = Set.empty
        let mutable cPLength = 0
        let mutable routingTableRow = Array.create 16 ("")
        let rec loop() = actor {
            let! msg = mailbox.Receive()
            match msg with
            | Msg (_)
                -> None |> ignore
            | Build(nId,numDigits) -> 
                id <- nId
                rows <- numDigits
                routingTable <- Array2D.zeroCreate rows cols
                itr <- 0
                number <- Int32.Parse(nId,Globalization.NumberStyles.HexNumber)
                left <- number
                right <- number
                while itr < 8 do
                    if left = 0 then
                        left <- actorMap.Count-1
                    leafSet <- leafSet.Add((string) left)
                    itr <- itr + 1
                    left <- left - 1
                while itr < 16 do
                    if right = actorMap.Count-1 then
                        right <- 0
                    leafSet <- leafSet.Add((string) right)
                    itr <- itr + 1
                    right <- right + 1
            | Update(routingRow) ->
                routingTable.[currentRow,*] <- routingRow
                currentRow <- currentRow + 1
            | Routing(key,source,hops) ->
                if key = id then
                    if actorHopsMap.ContainsKey(source) then
                        let mutable temp = actorHopsMap.[source]
                        let total =  double (actorHopsMap.Item(source).[1])
                        let avghops = double (actorHopsMap.Item(source).[0])
                        temp.[0] <- ((avghops * total) + (double) hops) / (total + 1.0)
                        temp.[1] <- total + 1.0
                        actorHopsMap <- actorHopsMap.Add(source, temp)
                    else
                        let tempArray = [| (float) hops; 1.0 |]
                        actorHopsMap <- actorHopsMap.Add(source,tempArray)
                elif leafSet.Contains(key) then
                    actorMap.Item(key) <! Routing(key,source,hops+1)
                else
                    let mutable i,j = 0,0 
                    while (key.[i] = id.[i]) do
                        i <- i + 1
                    cPLength <- i
                    let mutable rtrow = cPLength
                    let mutable rtcol = Int32.Parse(key.[cPLength].ToString(),Globalization.NumberStyles.HexNumber)
                    if isNull routingTable.[rtrow,rtcol] then
                        rtcol <- 0
                    actorMap.Item(routingTable.[rtrow,rtcol]) <! Routing(key,source,hops+1)
            | Join(key,currentIndex) ->    
                let mutable i,j,k = 0,0,currentIndex
                while key.[i] = id.[i] do
                    i <- i + 1
                cPLength <- i
                while k <= cPLength do
                    routingTableRow <- routingTable.[k, *]
                    let position = Int32.Parse(id.[cPLength].ToString(),Globalization.NumberStyles.HexNumber)
                    routingTableRow.[position] <- id
                    actorMap.[key] <! Update(routingTableRow)
                    k <- k+ 1
                rtrow <- cPLength
                rtcol <- Int32.Parse(key.[cPLength].ToString(),Globalization.NumberStyles.HexNumber)
                if isNull routingTable.[rtrow,rtcol] then
                    routingTable.[rtrow,rtcol] <- key
                else 
                    actorMap.[routingTable.[rtrow,rtcol]] <! Join(key,k)
            return! loop() 
        }
        loop()

let bossref (mailbox : Actor<_>) =
        let rec loop() = actor {
            let! msg = mailbox.Receive()
            match msg with
            | Msg(numNodes,numRequests) ->
                let mutable i = 1
                let mutable j = 1
                let mutable value = ""
                let mutable k = 1
                let mutable destinationId = ""
                let mutable ctr = 0
                let mutable totalHopSize = 0.0
                node <- String.replicate numDigits "0"
                let mutable child = spawn system (sprintf "child%s" node ) workerref
                child <! Build(node,numDigits)
                Thread.Sleep(5000)
                actorMap <- actorMap.Add(node,child)
                printfn "%s" "Starting to built the network"
                for i = 1 to numNodes - 1 do
                    if i = numNodes/2 then
                        printfn "%s" "Half of the network built is completed"
                    hexDecNum <- i.ToString("X")
                    hexDeclen <- hexDecNum.Length
                    node <- (String.replicate (numDigits - hexDeclen) "0") + hexDecNum
                    child <- spawn system (sprintf "child%s" node ) workerref
                    child <! Build(node,numDigits)
                    actorMap <- actorMap.Add(node,child)
                    actorMap.[String.replicate numDigits "0"] <! Join(node,0)
                    Thread.Sleep(50)
                Thread.Sleep(1000)
                printfn "%s" "Network is completly built"
                let mutable actorsArray = actorMap |> Map.toSeq |> Seq.map fst |> Seq.toArray
                while k <= numRequests do
                    for i = 0 to actorsArray.Length - 1 do
                        ctr <- ctr + 1
                        destinationId <- actorsArray.[i]
                        while destinationId = actorsArray.[i] do
                            destinationId <- actorsArray.[Random().Next(numNodes)]
                        actorMap.Item(actorsArray.[i]) <! Routing(destinationId,actorsArray.[i],0)
                        Thread.Sleep(5)
                    k <- k + 1
                Thread.Sleep(1000)
                printfn "Each Node has performed %d requests" numRequests 
                for entry in actorHopsMap do
                    totalHopSize <- totalHopSize + double entry.Value.[0]
                avgHopSize <- (totalHopSize / (double) actorHopsMap.Count)
                printfn ""
                printfn "Average Hop Size: %f" (avgHopSize)
                printfn ""
            | _ -> ()
            return! loop()
        }   
        loop()

numNodes <- int fsi.CommandLineArgs.[1]
numRequests <- int fsi.CommandLineArgs.[2]
numDigits <- int (ceil ( Math.Log(float numNodes,16.0)))
let boss = spawn system "Boss" bossref
boss <! Msg(numNodes,numRequests)
while not flag do
        if avgHopSize > 0.0 then
            flag <- true
