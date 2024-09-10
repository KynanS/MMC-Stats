import sqlite3
from typing import Dict, List, Set, Tuple
import pandas as pd
import json
import challonge
import datetime
import os
from credentials import userID, apiKey

players: List[Tuple] = [] # for storing player tuples to be put into the Player table in mmc.db
challongeNames: List[Tuple] = [] # for storing player challonge name tuples to be put into the ChallongeNames table in mmc.db 
mmc: List[Tuple] = [] # for storing the mmc tuples to be put into the MMC table in mmc.db
participants: List[Tuple] = [] # for storing the participant tuples to be put into the Participants table in mmc.db
matches: List[Tuple] = [] # for storing the matches tuples to be put into the Matches table in mmc.db
numOfMMC: int = 104 # the number of MMC's that have happend. Last updated September 10, 2024
playerRaces: Dict[str,List[str]] = dict() # for being able to reference a players race/offrace when inputing match data. List has main race first and offrace second
participantID: Dict[str, str] = dict() # for quick referencing a player name with their ID

def connect(dbName: str="mmc.db") -> sqlite3.Connection:
    """
    Function for connecting to a specific database.
    Default is mmc.db
    """
    return sqlite3.connect(dbName)

def pullMMCData() -> None:
    """
    Optional function used for repulling the data from all MMC's using the challonge API
    This function is only used if `repullData` in the main function is True. Default is False
    This function will go through and add all Matches and Participant data from the challonge website
    and put it into the `MMC` folder. 
    """
    # set challonge credentials
    challonge.set_credentials(userID, apiKey)
    # query the api for an index of all tournaments associated with the challonge account
    tournaments = challonge.tournaments.index()

    # query the api for the data for every mmc that has occured
    for edition in range(1, numOfMMC + 1):
        # get the id of current mmc in the loop
        for t in tournaments:
            if t["url"] == f"MagikarpMastersCup{edition}":
                tournamentID = t["id"]
        
        # try making the specific folder for the current loops mmc data
        try:
            os.mkdir(f"MMC/mmc{edition}")
        except:
            pass

        # get the participant data
        with open(f"MMC/mmc{edition}/participants.json", "w") as outfile:
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
        with open(f"MMC/mmc{edition}/matches.json", "w") as outfile:
            # get all the matches from a specific tournament
            matches = challonge.matches.index(tournamentID)
            # fix the date data for each match as sqlite does not support the format challonge writes it in
            for m in matches:
                for key in m.keys():
                    if type(m[key]) == datetime.datetime:
                        m[key] = m[key].strftime("%Y/%m/%d")
            # after cleaning the data, write the data to a json file
            outfile.write(json.dumps(matches, indent=4))

    return

def createTables(c: sqlite3.Cursor, conn: sqlite3.Connection) -> None:
    """
    Function for creating the tables in the sqlite database mmc.db
    :param c: an sqlite cursor object
    :param conn: an sqlite connection object
    """
    print("Creating Tables")
    # string for Player table specs
    playerTable: str =  """
                        CREATE TABLE IF NOT EXISTS Player(
                            NAME TEXT PRIMARY KEY,
                            MAINRACE TEXT,
                            COUNTRY TEXT,
                            TEAM TEXT,
                            OFFRACE TEXT
                        )
                        """
    # string for ChallongeNames table specs
    challongeNamesTable: str =  """
                        CREATE TABLE IF NOT EXISTS ChallongeNames(
                            CNAME TEXT PRIMARY KEY,
                            NAME TEXT NOT NULL,
                            FOREIGN KEY (NAME) REFERENCES Player (NAME)     
                        )
                        """
    # string for MMC table specs
    mmcTable: str =  """
                        CREATE TABLE IF NOT EXISTS MMC(
                            TOURNAMENTID TEXT PRIMARY KEY,
                            NUMBER INTEGER NOT NULL,
                            ELIMINATION TEXT NOT NULL,
                            ROUNDS INTEGER NOT NULL,
                            DATE TEXT NOT NULL
                        )
                        """
    # string for Participants table specs
    participantsTable: str =  """
                        CREATE TABLE IF NOT EXISTS Participants(
                            CHALLONGEID TEXT PRIMARY KEY,
                            CNAME TEXT NOT NULL,
                            ACCOUNTID TEXT NOT NULL,
                            TOURNAMENTID TEXT NOT NULL,
                            FOREIGN KEY (CNAME) REFERENCES ChallongeNames (CNAME),                                
                            FOREIGN KEY (TOURNAMENTID) REFERENCES MMC (TOURNAMENTID)                                
                        )
                        """
    # string for Matches table specs
    matchesTable: str =  """
                        CREATE TABLE IF NOT EXISTS Matches(
                            MATCHID TEXT PRIMARY KEY,
                            TOURNAMENTID TEXT NOT NULL,                          
                            WINNERID TEXT NOT NULL,
                            LOSERID TEXT NOT NULL,
                            WINNERSCORE INTEGER NOT NULL,
                            LOSERSCORE INTEGER NOT NULL,
                            ROUND INTEGER NOT NULL,
                            WINNERRACE TEXT,
                            LOSERRACE TEXT,
                            FOREIGN KEY (TOURNAMENTID) REFERENCES MMC (TOURNAMENTID),                                
                            FOREIGN KEY (WINNERID) REFERENCES Participants (CHALLONGEID),                                
                            FOREIGN KEY (LOSERID) REFERENCES Participants (CHALLONGEID)                                
                        )
                        """

    # create the tables
    # Player Table
    try:
        c.execute(playerTable)
        conn.commit()
    except:
        print("Player table did not create properly")
    # ChallongeNames Table
    try:
        c.execute(challongeNamesTable)
        conn.commit()
    except:
        print("Challonge Names table did not create properly")
    # MMC Table
    try:
        c.execute(mmcTable)
        conn.commit()
    except:
        print("MMC table did not create properly")
    # Participants Table
    try:
        c.execute(participantsTable)
        conn.commit()
    except:
        print("Participants table did not create properly")
    # Matches Table
    try:
        c.execute(matchesTable)
        conn.commit()
    except:
        print("Matches table did not create properly")
    
    print("Finished Creating Tables")
    return

def preparePlayerData(names: pd.DataFrame) -> None:
    """
    Function that uses the Names.csv file to store all relevant data for players like name, race, country, etc.
    Also prepare data for attaching a players standarized name with their challonge username(s)
    :param names: a pandas dataframe that holds everyones name info
    """
    print("Preparing Player Data")
    # keep track of which players have been seen so that no one is added multiple times
    playersSeen: Set[str] = set()
    # go through each name
    for ind in names.index:
        # Add each challonge username with standardized name
        challongeNames.append((str(names["Name"][ind]), str(names["Normal Name"][ind])))
        # if a player has not been added for the Player table, do so
        if names["Normal Name"][ind] not in playersSeen:
            # add theh name to names seen
            playersSeen.add(names["Normal Name"][ind])
            newPlayer: Tuple[str] = (str(names["Normal Name"][ind]), str(names["Race"][ind]), str(names["Country"][ind]), str(names["Team"][ind]), str(names["OffRace"][ind]))
            players.append(newPlayer)
        # update the playerRaces dict
        playerRaces[names["Name"][ind]] = [names["Race"][ind],names["OffRace"]]

    print("Finished Preparing Player Data")
    return

def preparePartsData(partsData) -> None:
    """
    Function for preparing the data to be put into the Participant table
    Data is being read from the participants.json files in the MMC folder
    :param partsData: a json loads object holding the participants.json data
    """
    print("Preparing Participant Data")
    # go through each participant from the tournament being looked at
    for participant in partsData:
        # not every "participant" checked in. The ones who did have a final rank, so this if statement filters out anyone who signed up but didn't check in
        if participant["final_rank"]:
            person: Tuple[str] = (str(participant["id"]), str(participant["name"]), str(participant["challonge_user_id"]), str(participant["tournament_id"]))
            participants.append(person)
        # save id with name
        try:
            participantID[str(participant["id"])] = participant["name"]
        except:
            continue
    
    print("Finished Preparing Participant Data")
    return

def prepareMatchData(matchesData) -> Tuple[str, int]:
    """
    Function for preparing the data to put into the Matches table
    The data is found in the matches.json files in the MMC folder
    :param matchesData: a json loads object containing the data from a matches.json file
    :returns: a tuple of 1 string and 1 integer, the string being the elimination style (single or double) 
    and the integer being the number of rounds the tournament had
    """
    print("Preparing Match Data")
    rounds = 0 # for keeping track of how many rounds were in the tournament
    elim = "s" # to indicate the elimination style, default is s (single), but could be changed to d (double)
    # go through every match
    for match in matchesData:
        if match["state"] == "complete":
            skip = False # sometimes a match is not played or needed but is still included in the data. This turns true if one of those matches is encountered
            matchID: str = str(match["id"])
            tournamentID: str = str(match["tournament_id"])
            winnerID: str = str(match["winner_id"])
            loserID: str = str(match["loser_id"])
            winnerName: str = str(participantID[winnerID])
            loserName: str = str(participantID[loserID])
            winnerRace: str = str(playerRaces[winnerName][0])
            loserRace: str = str(playerRaces[loserName][0])
            # use the scoreFix function to standardize the score as challonge is funny with how scores are recorded
            winnerScore, loserScore = scoreFix(match["scores_csv"])
            # if a fake match is found, winnerScore is set to -2 to indicate it should not be added to the Matches table
            if winnerScore == -2:
                skip = True
            mRound: int = int(match["round"])
            # check to see if the current match that's being looked at is the highest round match or not
            if rounds < mRound:
                rounds = mRound
            # loser bracket rounds are labeled as a negative, so if a negative round is encountered, we know this tournament was double elim
            if mRound < 0:
                elim = "d"
            # add the match to the matches list
            if not skip:
                matches.append((matchID, tournamentID, winnerID, loserID, winnerScore, loserScore, mRound, winnerRace, loserRace))

    print("Finished Preparing Match Data")
    return elim, rounds

def insertPlayerData(c: sqlite3.Cursor, conn: sqlite3.Connection) -> None:
    """
    Function for adding populating the Player sqlite table
    :param c: sqlite cursor
    :param conn: sqlite connection
    """
    print("Inserting Player Data")
    # attempt to add the prepared data into the Player table
    try:
        c.executemany("INSERT INTO Player VALUES (?,?,?,?,?)", players)
        conn.commit()
        print("Finished Inserting Player Data")
    # if there was an error, indicate there was an error inserting this data specifically so a solution can be found
    except:
        print("Inserting Player data didn't work")
    return

def insertMMCData(c: sqlite3.Cursor, conn: sqlite3.Connection) -> None:
    """
    Function for adding populating the MMC sqlite table
    :param c: sqlite cursor
    :param conn: sqlite connection
    """
    print("Inserting MMC Data")
    # attempt to add the prepared data into the MMC table
    try:
        c.executemany("INSERT INTO MMC VALUES (?,?,?,?,?)", mmc)
        conn.commit()
        print("Finished inserting MMC Data")
    # if there was an error, indicate there was an error inserting this data specifically so a solution can be found
    except:    
        print("Inserting MMC data didn't work")
    return

def insertChallongeNameData(c: sqlite3.Cursor, conn: sqlite3.Connection) -> None:
    """
    Function for adding populating the ChallongeNames sqlite table
    :param c: sqlite cursor
    :param conn: sqlite connection
    """
    print("Inserting Challonge Name Data")
    # attempt to add the prepared data into the MMC table
    try:
        c.executemany("INSERT INTO ChallongeNames VALUES (?,?)", challongeNames)
        conn.commit()
        print("Finished Inserting Challonge Name Data")
    # if there was an error, indicate there was an error inserting this data specifically so a solution can be found
    except:
        print("Inserting Challonge Name data didn't work")
    return

def insertPartsData(c: sqlite3.Cursor, conn: sqlite3.Connection) -> None:
    """
    Function for adding populating the Participants sqlite table
    :param c: sqlite cursor
    :param conn: sqlite connection
    """
    print("Inserting Participants Data")
    # attempt to add the prepared data into the Participants table
    try:
        c.executemany("INSERT INTO Participants VALUES (?,?,?,?)", participants)
        conn.commit()
        print("Finished Inserting Participants Data")
    # if there was an error, indicate there was an error inserting this data specifically so a solution can be found
    except:
        print("Inserting Participants data didn't work")
    return

def insertMatchData(c: sqlite3.Cursor, conn: sqlite3.Connection) -> None:
    """
    Function for adding populating the Matches sqlite table
    :param c: sqlite cursor
    :param conn: sqlite connection
    """
    print("Inserting Match Data")
    # attempt to add the prepared data into the Matches table
    try:
        c.executemany("INSERT INTO Matches VALUES (?,?,?,?,?,?,?,?,?)", matches)
        conn.commit()
        print("Finished Inserting Match Data")
    # if there was an error, indicate there was an error inserting this data specifically so a solution can be found
    except:
        print("Inserting Match data did not work")
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

def main() -> None:
    # if the data needs to be repulled from challonge, set to True
    repullData = False
    if repullData:
        pullMMCData()
    # if you need to remake the database due to a change in the data, set to True
    startFromScratch: bool = True
    # connect to db
    conn: sqlite3.Connection = connect()
    # create a cursor
    c: sqlite3.Cursor = conn.cursor()
    
    if startFromScratch:
        # create the tables
        createTables(c, conn)    
    
        # load in the names
        names: pd.DataFrame = pd.read_csv("Names.csv",encoding="utf-16",delimiter="\t")
        
        # prepare data for player table
        preparePlayerData(names)
        
        # input data from MMC's
        for edition in range(1, numOfMMC + 1):
            print(edition)
            # load data from json files
            matchesFile = open(f"MMC/mmc{edition}/matches.json", "r")
            matchesData = json.loads(matchesFile.read())
            partsFile = open(f"MMC/mmc{edition}/participants.json", "r")
            partsData = json.loads(partsFile.read())
            matchesFile.close()
            partsFile.close()
            
            # data for MMC table
            tournamentID: str = str(matchesData[0]["tournament_id"])
            date: str = str(matchesData[0]["started_at"])

            # data for Participants table
            preparePartsData(partsData)

            # data for Matches table
            elim, rounds = prepareMatchData(matchesData)

            # add data for MMC table
            mmc.append((tournamentID, edition, elim, rounds, date))
        
        # input data into db
        insertPlayerData(c, conn)
        insertMMCData(c, conn)
        insertChallongeNameData(c, conn)
        insertPartsData(c, conn)
        insertMatchData(c, conn)

        print("Database creation complete")

    conn.close()

if __name__ == "__main__":
    try:
        main()
    except:
        exit(1)