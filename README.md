Loading Process
==================

Details from start to finish how the program works to retrieve data from ESPN.

* Initialize
    * Based on selection, chooses which league to begin processing.

* Start
    * Parses the url for the selected league.

* Parse_Match
    * Parses information about the game such as teams and scores.

* Process_(league)_Stats
    * Parses team and player stats for each game.

* Insert_Or_Update_Game
    * Inserts data into the Games table.