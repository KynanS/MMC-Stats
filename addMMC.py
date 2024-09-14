import sqlite3
from typing import Dict, List, Tuple
import challonge
import os
import datetime
import json
import pandas as pd
from credentials import userID, apiKey

mmc: int = 106
playerRaces: Dict[str,List[str]] = dict() # for being able to reference a players race/offrace when inputing match data. List has main race first and offrace second
participantID: Dict[str, str] = dict() # for quick referencing a player name with their ID

def connect(dbName: str="mmc.db") -> sqlite3.Connection:
    return sqlite3.connect(dbName)

def pullMMCData() -> None:
    """
    Function used for pulling the data from a specific MMC using the challonge API
    This function will pull the participant and matches data from the specified MMC
    and put it into the MMC folder
    """
    # set challonge credentials
    challonge.set_credentials(userID, apiKey)
    # query the api for an index of all tournaments associated with the challonge account
    tournaments = challonge.tournaments.index()
    # query the api for an index of all tournaments associated with the challonge account
    tourney: str = f"MagikarpMastersCup{mmc}"
    # find the tournament id for the MMC in question
    for t in tournaments:
        if tourney == t["url"]:
            tournamentID = t["id"]
            break
    # try making the specific folder for this MMC
    try:
        os.mkdir(f"MMC/mmc{mmc}")
    except:
        pass
    # get the participant data
    with open(f"MMC/mmc{mmc}/participants.json", "w") as outfile:
        # get all the participants from a specific tournament
        participants = challonge.participants.index(tournamentID)
        # fix the date data for each participant as sqlite does not support the format challonge writes it in
        for p in participants:
            for key in p.keys():
                if type(p[key]) == datetime.datetime:
                    # use year, month, day
                    p[key] = p[key].strftime("%Y/%m/%d")
        # after cleaning the data, write the data to a json file
        outfile.write(json.dumps(participants, indent=4))
    # get the match data   
    with open(f"MMC/mmc{mmc}/matches.json", "w") as outfile:
        # get all the matches from a specific tournament
        matches = challonge.matches.index(tournamentID)
        for m in matches:
            for key in m.keys():
                if type(m[key]) == datetime.datetime:
                    m[key] = m[key].strftime("%Y/%m/%d")
        # after cleaning the data, write the data to a json file
        outfile.write(json.dumps(matches, indent=4))

    return

def newChallongeNames(c: sqlite3.Cursor, conn: sqlite3.Connection) -> None:
    """
    Function for checking to see if there are any new players that have never participated before.
    Checks all challonge usernames to see if they have been seen before. 
    If not, those names are printed out and the program is stopped.
    Go add those names to Names.csv and rerun the script.
    :param c: an sqlite3 cursor
    :param conn: an sqlite3 connection
    """
    # load the participants.json file
    partsFile = open(f"MMC/mmc{mmc}/participants.json", "r")
    partsData = json.loads(partsFile.read())
    # load the Names.csv file to check for new names
    namesFile = pd.read_csv("Names.csv",encoding="utf-16",delimiter="\t")
    names = namesFile["Name"].to_list()
    newNames = set()
    # get every username from the json file
    for p in partsData:
        newNames.add(p["name"])
    # check each name to see if it is new
    end = False
    for name in newNames:
        if name not in names:
            print(name)
            end = True

    partsFile.close()
    if end:
        conn.close()
        exit(1)
    else:
        return

def scoreFix(score: str) -> Tuple[int, int]:
    """
    A function to convert the scores displayed in challonge from a matches.json file into a standardized integer
    :param score: a string that is the score from a match from a matches.json file
    :returns: a tuple of two integers, the first being the map score of the winner, second being the map score of the loser
    """
    winner: int = -2 # -2 is returned if there is no score from a match which then triggers a skip in prepareMatchData
    loser: int = -2
    
    walkovers: List[str] = ["0--1", "2-99", "990-0", "69-0", "-99-99", "99-0", 
                            "0-99", "0-0", "0--99", "99-1",
                            "0-98", "-1-0"] # all different styles of walkover scores used by players
    oneZero: List[str] = ["0-1", "1-0"] # all different strings for a 1-0 result
    twoZero: List[str] = ["1-0,1-0", "0-2", "0-1,0-1", "2-0"] # all different strings for a 2-0 result
    twoOne: List[str] = ["0-1,1-0,0-1", "1-2", "2-1", "0-1,1-0,1-0"] # all different strings for a 2-1 result
    threeZero: List[str] = ["3-0", "0-3"] # all different strings for a 3-0 result
    threeOne: List[str] = ["0-1,0-1,1-0,0-1", "3-1", "1-0,0-1,0-1,0-1", "1-3"] # all different strings for a 3-1 result
    threeTwo: List[str] = ["2-3", "3-2"] # all different strings for a 3-2 result
    # find out what the score was and standardize it
    if score in twoZero:
        winner = 2
        loser = 0
    elif score in twoOne:
        winner = 2
        loser = 1
    elif score in oneZero:
        winner = 1
        loser = 0
    elif score in walkovers:
        winner = 0
        loser = -1
    elif score in threeZero:
        winner = 3
        loser = 0
    elif score in threeOne:
        winner = 3
        loser = 1
    elif score in threeTwo:
        winner = 3
        loser = 2
    # if it's a blank score return -2
    elif score == "":
        return winner, loser
    # if it's a score that's never been seen before, print it so it can be added
    else:
        print(f"{score} is unknown to the system")

    return winner, loser

def insertData(c: sqlite3.Cursor, conn: sqlite3.Connection) -> None:
    """
    Function to insert the data for the new MMC into the mmc.db sqlite database
    :param c: sqlite3 cursor
    :param conn: sqlite3 connection
    """
    # load names csv file
    namesFile = pd.read_csv("Names.csv",encoding="utf-16",delimiter="\t")

    # data to be pulled from participants file
    with open(f"MMC/mmc{mmc}/participants.json", "r") as file:
        # load participant data
        partsData = json.loads(file.read())
        
        # enter participant data
        print("Entering participant data")
        for p in partsData:
            try:
                name = p["name"]
                # get data from names csv                
                namesData = namesFile[namesFile["Name"] == name]
                ind: int = namesData.index[0]
                normalName = namesFile["Normal Name"][ind]
                playerRaces[name] = [namesFile["Race"][ind], namesFile["OffRace"][ind]]
                participantID[str(p["id"])] = name

                # insert data into ChallongeNames
                c.execute("SELECT * FROM ChallongeNames WHERE CNAME = ?", (name,))
                results = c.fetchall()
                # if the results are empty, we need to add that person into the table
                if len(results) == 0:               
                    # entry for ChallongeNames
                    cnames = (str(namesFile["Name"][ind]), str(namesFile["Normal Name"][ind]))
                    c.executemany("INSERT INTO ChallongeNames VALUES (?,?)", (cnames,))
                    conn.commit()
                
                # insert data into Player
                c.execute("SELECT * FROM Player WHERE NAME = ?", (normalName,))
                results = c.fetchall()
                # the results are empty, we need to add that person into the table
                if len(results) == 0:
                    # entry for Player
                    player: Tuple[str] = (str(namesFile["Normal Name"][ind]), str(namesFile["Race"][ind]), str(namesFile["Country"][ind]), str(namesFile["Team"][ind]), str(namesFile["OffRace"][ind]))
                    c.executemany("INSERT INTO Player VALUES (?,?,?,?,?)", (player,))
                    conn.commit()
                
                # insert data into Participants table
                pid = str(p["id"])
                c.execute("SELECT * FROM Participants WHERE CHALLONGEID = ?", (pid,))
                results = c.fetchall()
                # only add participant if they do not yet exist
                if len(results) == 0:
                    participant = (str(p["id"]), str(p["name"]), str(p["challonge_user_id"]), str(p["tournament_id"]))
                    c.executemany("INSERT INTO Participants VALUES (?,?,?,?)", (participant,))
                    conn.commit()
            except:
                print(f"{name} was not added")
        print("Finished entering participant data")
    
    # data to be pulled from matches file
    with open(f"MMC/mmc{mmc}/matches.json", "r") as file:
        print("Entering Match Data")
        matchesData = json.loads(file.read())
        # things for MMC table
        tournamentID = str(matchesData[0]["tournament_id"])
        date = str(matchesData[0]["started_at"])
        rounds = 0
        elim = "s"
        
        # go through each match and add it to the table
        for m in matchesData:
            if m["state"] == "complete":
                skip = False
                matchID: str = str(m["id"])
                # make sure that the match doesn't already exist before doing more work
                c.execute("SELECT * FROM Matches WHERE MATCHID = ?", (matchID,))
                results = c.fetchall()
                if len(results) == 0:
                    winnerID: str = str(m["winner_id"])
                    loserID: str = str(m["loser_id"])
                    winnerName: str = str(participantID[winnerID])
                    loserName: str = str(participantID[loserID])
                    winnerRace: str = str(playerRaces[winnerName][0])
                    loserRace: str = str(playerRaces[loserName][0])
                    winnerScore, loserScore = scoreFix(m["scores_csv"])
                    if winnerScore == -2:
                        print(m["scores_csv"])
                        skip = True
                    mRound: int = int(m["round"])
                    if rounds < mRound:
                        rounds = mRound
                    if mRound < 0:
                        elim = "d"
                    # if it is a legit match, add it
                    if not skip:
                        match = (matchID, tournamentID, winnerID, loserID, winnerScore, loserScore, mRound, winnerRace, loserRace)
                        c.executemany("INSERT INTO Matches VALUES (?,?,?,?,?,?,?,?,?)", (match,))
                        conn.commit()                 
        print("Finished entering match data")

        # check if there is already an entry for this MMC
        c.execute("SELECT * FROM MMC WHERE NUMBER = ?", (mmc,))
        results = c.fetchall()
        # if it isn't in the table yet, add it
        if len(results) == 0:
            edition = (tournamentID, mmc, elim, rounds, date)
            c.executemany("INSERT INTO MMC VALUES (?,?,?,?,?)", (edition,))
            conn.commit()
    
    return

def main() -> None:
    # connect to db
    conn: sqlite3.Connection = connect()
    # create a cursor
    c: sqlite3.Cursor = conn.cursor()

    # add the MMC to pull data for
    print("Pulling MMC Data")
    pullMMCData()
    print("Finished Pulling MMC Data")
    
    # check for new challonge usernames
    print("Checking for new names")
    newChallongeNames(c, conn)
    print("Finished checking for new names")
    
    # insert new data
    insertData(c, conn)

    print("Data Included")
    conn.close()



if __name__ == "__main__":
    try:
        main()
    except:
        exit(1)