class CreateStatisticsDatabase < ActiveRecord::Migration
  def change
    # Create tables, indexes and foreign keys
    create_table "Games", primary_key: "GameID", force: :cascade do |t|
      t.integer  "LeagueID",     limit: 11,                    null: false
      t.string   "GameTitle",    limit: 150
      t.integer  "HomeTeamID",   limit: 4
      t.integer  "AwayTeamID",   limit: 4
      t.integer  "Attendance",   limit: 4,   default: 0
      t.datetime "StartDate"
      t.binary   "InProgress",   limit: 1,   default: 0, null: false
      t.string   "ESPNUrl",      limit: 150,                  null: false
      t.datetime "CreatedDate",                               null: false
      t.datetime "ModifiedDate",                              null: false
    end

    add_index "Games", ["AwayTeamID"], name: "index_matches_on_away_team_id", using: :btree
    add_index "Games", ["HomeTeamID"], name: "index_matches_on_home_team_id", using: :btree

    create_table "Leagues", primary_key: "LeagueID", force: :cascade do |t|
      t.string  "LeagueName", limit: 50
      t.integer "SportID",    limit: 1
      t.string  "SportName",  limit: 50
    end

    create_table "PlayerStats_Baseball", id: false, force: :cascade do |t|
      t.integer  "GameID",             limit: 11,             null: false
      t.integer  "LeagueID",           limit: 11,             null: false
      t.integer  "TeamID",             limit: 4,             null: false
      t.integer  "PlayerID",           limit: 4,             null: false
      t.integer  "AtBats",             limit: 1, default: 0
      t.integer  "Runs",               limit: 1, default: 0
      t.integer  "Hits",               limit: 1, default: 0
      t.integer  "RBI",                limit: 1, default: 0
      t.integer  "HomeRuns",           limit: 1, default: 0
      t.integer  "Walks",              limit: 1, default: 0
      t.integer  "Strikeouts",         limit: 1, default: 0
      t.integer  "StolenBases",        limit: 1, default: 0
      t.integer  "PitchingInnings",    limit: 1, default: 0
      t.integer  "PitchingHits",       limit: 1, default: 0
      t.integer  "PitchingRuns",       limit: 1, default: 0
      t.integer  "PitchingEarnedRuns", limit: 1, default: 0
      t.integer  "PitchingWalks",      limit: 1, default: 0
      t.integer  "PitchingStrikeouts", limit: 1, default: 0
      t.integer  "PitchingHomeRuns",   limit: 1, default: 0
      t.datetime "CreatedDate"
      t.datetime "ModifiedDate"
    end

    add_index "PlayerStats_Baseball", ["GameID"], name: "GameID", using: :btree
    add_index "PlayerStats_Baseball", ["LeagueID"], name: "LeagueID", using: :btree
    add_index "PlayerStats_Baseball", ["PlayerID"], name: "PlayerID", using: :btree
    add_index "PlayerStats_Baseball", ["TeamID"], name: "TeamID", using: :btree

    create_table "PlayerStats_Basketball", id: false, force: :cascade do |t|
      t.integer  "GameID",         limit: 11,             null: false
      t.integer  "LeagueID",       limit: 11,             null: false
      t.integer  "TeamID",         limit: 4,             null: false
      t.integer  "PlayerID",       limit: 4,             null: false
      t.integer  "FGTaken",        limit: 1, default: 0
      t.integer  "FGMade",         limit: 1, default: 0
      t.integer  "FGPercent",      limit: 1, default: 0
      t.integer  "ThreePtTaken",   limit: 1, default: 0
      t.integer  "ThreePtMade",    limit: 1, default: 0
      t.integer  "ThreePtPercent", limit: 1, default: 0
      t.integer  "FTTaken",        limit: 1, default: 0
      t.integer  "FTMade",         limit: 1, default: 0
      t.integer  "FTPercent",      limit: 1, default: 0
      t.integer  "OffRebounds",    limit: 1, default: 0
      t.integer  "DefRebounds",    limit: 1, default: 0
      t.integer  "Assists",        limit: 1, default: 0
      t.integer  "Turnovers",      limit: 1, default: 0
      t.integer  "Steals",         limit: 1, default: 0
      t.integer  "Blocks",         limit: 1, default: 0
      t.integer  "BlocksAgainst",  limit: 1, default: 0
      t.integer  "PersonalFouls",  limit: 1, default: 0
      t.datetime "CreatedDate"
      t.datetime "ModifiedDate"
    end

    add_index "PlayerStats_Basketball", ["GameID"], name: "GameID", using: :btree
    add_index "PlayerStats_Basketball", ["LeagueID"], name: "LeagueID", using: :btree
    add_index "PlayerStats_Basketball", ["PlayerID"], name: "PlayerID", using: :btree
    add_index "PlayerStats_Basketball", ["TeamID"], name: "TeamID", using: :btree

    create_table "PlayerStats_Football", id: false, force: :cascade do |t|
      t.integer  "GameID",                limit: 11,                                        null: false
      t.integer  "LeagueID",              limit: 11,                                        null: false
      t.integer  "TeamID",                limit: 4,                                        null: false
      t.integer  "PlayerID",              limit: 4,                                        null: false
      t.integer  "PassingCompletions",    limit: 1,                          default: 0
      t.integer  "PassingAttempts",       limit: 1,                          default: 0
      t.integer  "PassingYards",          limit: 1,                          default: 0
      t.integer  "PassingCompletionsPct", limit: 1,                          default: 0
      t.integer  "PassingSacks",          limit: 1,                          default: 0
      t.integer  "PassingTDs",            limit: 1,                          default: 0
      t.integer  "PassingInterceptions",  limit: 1,                          default: 0
      t.integer  "PassingRating",         limit: 1,                          default: 0
      t.integer  "RushingAttemps",        limit: 1,                          default: 0
      t.integer  "RushingYards",          limit: 1,                          default: 0
      t.integer  "RushingLong",           limit: 1,                          default: 0
      t.integer  "RushingTDs",            limit: 1,                          default: 0
      t.integer  "ReceivingCatches",      limit: 1,                          default: 0
      t.integer  "ReceivingTargets",      limit: 1,                          default: 0
      t.integer  "ReceivingYards",        limit: 1,                          default: 0
      t.integer  "ReceivingLong",         limit: 1,                          default: 0
      t.integer  "ReceivingTDs",          limit: 1,                          default: 0
      t.integer  "FumblesTotal",          limit: 1,                          default: 0
      t.integer  "FumblesLost",           limit: 1,                          default: 0
      t.integer  "KickingXPMade",         limit: 1,                          default: 0
      t.integer  "KickingXPAttempts",     limit: 1,                          default: 0
      t.integer  "KickingFGMade",         limit: 1,                          default: 0
      t.integer  "KickingFGAttempts",     limit: 1,                          default: 0
      t.integer  "KickingLong",           limit: 1,                          default: 0
      t.integer  "KickingPoints",         limit: 1,                          default: 0
      t.integer  "Punts",                 limit: 1,                          default: 0
      t.integer  "KickReturns",           limit: 1,                          default: 0
      t.integer  "KickReturnYards",       limit: 1,                          default: 0
      t.integer  "KickReturnLong",        limit: 1,                          default: 0
      t.integer  "KickReturnTDs",         limit: 1,                          default: 0
      t.integer  "PuntReturns",           limit: 1,                          default: 0
      t.integer  "PuntReturnYards",       limit: 1,                          default: 0
      t.integer  "PuntReturnLong",        limit: 1,                          default: 0
      t.integer  "PuntReturnTDs",         limit: 1,                          default: 0
      t.integer  "DefenseTackles",        limit: 1,                          default: 0
      t.integer  "DefenseAssists",        limit: 1,                          default: 0
      t.decimal  "DefenseSacks",                    precision: 12, scale: 2, default: 0.0
      t.integer  "DefensePassesDefended", limit: 1,                          default: 0
      t.integer  "DefenseInterceptions",  limit: 1,                          default: 0
      t.integer  "DefenseIntYards",       limit: 1,                          default: 0
      t.integer  "DefenseIntTDs",         limit: 1,                          default: 0
      t.datetime "CreatedDate"
      t.datetime "ModifiedDate"
    end

    add_index "PlayerStats_Football", ["GameID"], name: "GameID", using: :btree
    add_index "PlayerStats_Football", ["LeagueID"], name: "LeagueID", using: :btree
    add_index "PlayerStats_Football", ["PlayerID"], name: "PlayerID", using: :btree
    add_index "PlayerStats_Football", ["TeamID"], name: "TeamID", using: :btree

    create_table "PlayerStats_Golf", id: false, force: :cascade do |t|
      t.integer  "GameID",       limit: 11,                  null: false
      t.integer  "LeagueID",     limit: 11,                  null: false
      t.integer  "PlayerID",     limit: 4,                  null: false
      t.integer  "Position",     limit: 1, default: 0
      t.integer  "Round_1",      limit: 1, default: 0
      t.integer  "Round_2",      limit: 1, default: 0
      t.integer  "Round_3",      limit: 1, default: 0
      t.integer  "Round_4",      limit: 1, default: 0
      t.integer  "ToPar",        limit: 1, default: 0
      t.integer  "Strokes",      limit: 4, default: 0
      t.binary   "MissedCut",    limit: 1, default: 0
      t.datetime "CreatedDate"
      t.datetime "ModifiedDate"
    end

    create_table "PlayerStats_Hockey", id: false, force: :cascade do |t|
      t.integer  "GameID",         limit: 11,             null: false
      t.integer  "LeagueID",       limit: 11,             null: false
      t.integer  "TeamID",         limit: 4,             null: false
      t.integer  "PlayerID",       limit: 4,             null: false
      t.integer  "ShotsAgainst",   limit: 1, default: 0
      t.integer  "GoalsAgainst",   limit: 1, default: 0
      t.integer  "Saves",          limit: 1, default: 0
      t.integer  "Goals",          limit: 1, default: 0
      t.integer  "Assists",        limit: 1, default: 0
      t.integer  "Points",         limit: 1, default: 0
      t.integer  "PenaltyMinutes", limit: 1, default: 0
      t.integer  "ShotsOnGoal",    limit: 1, default: 0
      t.integer  "Blocks",         limit: 1, default: 0
      t.integer  "Hits",           limit: 1, default: 0
      t.integer  "Takeaways",      limit: 1, default: 0
      t.integer  "Giveaways",      limit: 1, default: 0
      t.integer  "FaceoffsWon",    limit: 1, default: 0
      t.integer  "FaceoffsLost",   limit: 1, default: 0
      t.integer  "FaceoffPercent", limit: 1, default: 0
      t.datetime "CreatedDate"
      t.datetime "ModifiedDate"
    end

    add_index "PlayerStats_Hockey", ["GameID"], name: "GameID", using: :btree
    add_index "PlayerStats_Hockey", ["LeagueID"], name: "LeagueID", using: :btree
    add_index "PlayerStats_Hockey", ["PlayerID"], name: "PlayerID", using: :btree
    add_index "PlayerStats_Hockey", ["TeamID"], name: "TeamID", using: :btree

    create_table "PlayerStats_Racing", id: false, force: :cascade do |t|
      t.integer  "GameID",       limit: 11,             null: false
      t.integer  "LeagueID",     limit: 11,             null: false
      t.integer  "PlayerID",     limit: 4,             null: false
      t.integer  "StartPlace",   limit: 1, default: 0
      t.integer  "FinishPlace",  limit: 1, default: 0
      t.integer  "LapsComplete", limit: 4, default: 0
      t.integer  "LapsLed",      limit: 4, default: 0
      t.integer  "Winnings",     limit: 4, default: 0
      t.datetime "CreatedDate"
      t.datetime "ModifiedDate"
    end

    create_table "PlayerStats_Soccer", id: false, force: :cascade do |t|
      t.integer  "GameID",         limit: 11,             null: false
      t.integer  "LeagueID",       limit: 11,             null: false
      t.integer  "TeamID",         limit: 4,             null: false
      t.integer  "PlayerID",       limit: 4,             null: false
      t.integer  "Goals",          limit: 1, default: 0
      t.integer  "Saves",          limit: 1, default: 0
      t.integer  "Assists",        limit: 1, default: 0
      t.integer  "ShotsTotal",     limit: 1, default: 0
      t.integer  "ShotsOnGoal",    limit: 1, default: 0
      t.integer  "Offsides",       limit: 1, default: 0
      t.integer  "FoulsDrawn",     limit: 1, default: 0
      t.integer  "FoulsCommitted", limit: 1, default: 0
      t.integer  "YellowCards",    limit: 1, default: 0
      t.integer  "RedCards",       limit: 1, default: 0
      t.datetime "CreatedDate"
      t.datetime "ModifiedDate"
    end

    add_index "PlayerStats_Soccer", ["GameID"], name: "GameID", using: :btree
    add_index "PlayerStats_Soccer", ["LeagueID"], name: "LeagueID", using: :btree
    add_index "PlayerStats_Soccer", ["PlayerID"], name: "PlayerID", using: :btree
    add_index "PlayerStats_Soccer", ["TeamID"], name: "TeamID", using: :btree

    create_table "Players", primary_key: "PlayerID", force: :cascade do |t|
      t.string   "PlayerName",   limit: 100
      t.integer  "TeamID",       limit: 4
      t.integer  "LeagueID",     limit: 11
      t.string   "ESPNURL",      limit: 150
      t.datetime "CreatedDate",              null: false
      t.datetime "ModifiedDate",             null: false
    end

    add_index "Players", ["TeamID"], name: "TeamID", using: :btree

    create_table "TeamStats_Baseball", id: false, force: :cascade do |t|
      t.integer  "GameID",       limit: 11,             null: false
      t.integer  "LeagueID",     limit: 11,             null: false
      t.integer  "TeamID",       limit: 4,             null: false
      t.integer  "Inning_1",     limit: 1, default: 0
      t.integer  "Inning_2",     limit: 1, default: 0
      t.integer  "Inning_3",     limit: 1, default: 0
      t.integer  "Inning_4",     limit: 1, default: 0
      t.integer  "Inning_5",     limit: 1, default: 0
      t.integer  "Inning_6",     limit: 1, default: 0
      t.integer  "Inning_7",     limit: 1, default: 0
      t.integer  "Inning_8",     limit: 1, default: 0
      t.integer  "Inning_9",     limit: 1, default: 0
      t.integer  "Inning_10",    limit: 1, default: 0
      t.integer  "Inning_11",    limit: 1, default: 0
      t.integer  "Inning_12",    limit: 1, default: 0
      t.integer  "Inning_13",    limit: 1, default: 0
      t.integer  "Inning_14",    limit: 1, default: 0
      t.integer  "Runs",         limit: 1, default: 0
      t.integer  "Hits",         limit: 1, default: 0
      t.integer  "Errors",       limit: 1, default: 0
      t.datetime "CreatedDate"
      t.datetime "ModifiedDate"
    end

    add_index "TeamStats_Baseball", ["GameID"], name: "GameID", using: :btree
    add_index "TeamStats_Baseball", ["LeagueID"], name: "LeagueID", using: :btree
    add_index "TeamStats_Baseball", ["TeamID"], name: "TeamID", using: :btree

    create_table "TeamStats_Basketball", id: false, force: :cascade do |t|
      t.integer  "GameID",         limit: 11,             null: false
      t.integer  "LeagueID",       limit: 11,             null: false
      t.integer  "TeamID",         limit: 4,             null: false
      t.integer  "Quarter_1",      limit: 1, default: 0
      t.integer  "Quarter_2",      limit: 1, default: 0
      t.integer  "Quarter_3",      limit: 1, default: 0
      t.integer  "Quarter_4",      limit: 1, default: 0
      t.integer  "Overtime_1",     limit: 1, default: 0
      t.integer  "Overtime_2",     limit: 1, default: 0
      t.integer  "Overtime_3",     limit: 1, default: 0
      t.integer  "Half_1",         limit: 1, default: 0
      t.integer  "Half_2",         limit: 1, default: 0
      t.integer  "FinalScore",     limit: 4, default: 0
      t.integer  "FGTaken",        limit: 1, default: 0
      t.integer  "FGMade",         limit: 1, default: 0
      t.integer  "FGPercent",      limit: 1, default: 0
      t.integer  "ThreePtTaken",   limit: 1, default: 0
      t.integer  "ThreePtMade",    limit: 1, default: 0
      t.integer  "ThreePtPercent", limit: 1, default: 0
      t.integer  "FTTaken",        limit: 1, default: 0
      t.integer  "FTMade",         limit: 1, default: 0
      t.integer  "FTPercent",      limit: 1, default: 0
      t.integer  "OffRebounds",    limit: 1, default: 0
      t.integer  "DefRebounds",    limit: 1, default: 0
      t.integer  "Assists",        limit: 1, default: 0
      t.integer  "Turnovers",      limit: 1, default: 0
      t.integer  "Steals",         limit: 1, default: 0
      t.integer  "Blocks",         limit: 1, default: 0
      t.integer  "BlocksAgainst",  limit: 1, default: 0
      t.integer  "PersonalFouls",  limit: 1, default: 0
      t.datetime "CreatedDate"
      t.datetime "ModifiedDate"
    end

    add_index "TeamStats_Basketball", ["GameID"], name: "GameID", using: :btree
    add_index "TeamStats_Basketball", ["LeagueID"], name: "LeagueID", using: :btree
    add_index "TeamStats_Basketball", ["TeamID"], name: "TeamID", using: :btree

    create_table "TeamStats_Football", id: false, force: :cascade do |t|
      t.integer  "GameID",            limit: 11,             null: false
      t.integer  "LeagueID",          limit: 11,             null: false
      t.integer  "TeamID",            limit: 4,             null: false
      t.integer  "Quarter_1",         limit: 1, default: 0
      t.integer  "Quarter_2",         limit: 1, default: 0
      t.integer  "Quarter_3",         limit: 1, default: 0
      t.integer  "Quarter_4",         limit: 1, default: 0
      t.integer  "Overtime_1",        limit: 1, default: 0
      t.integer  "Overtime_2",        limit: 1, default: 0
      t.integer  "FinalScore",        limit: 1, default: 0
      t.integer  "FirstDowns",        limit: 1, default: 0
      t.integer  "TotalYards",        limit: 4, default: 0
      t.integer  "Turnovers",         limit: 1, default: 0
      t.integer  "TotalPlays",        limit: 1, default: 0
      t.integer  "RushingYards",      limit: 4, default: 0
      t.integer  "TotalRushes",       limit: 1, default: 0
      t.integer  "TotalPassingYards", limit: 4, default: 0
      t.integer  "Sacks",             limit: 1, default: 0
      t.integer  "Interceptions",     limit: 1, default: 0
      t.integer  "Punts",             limit: 1, default: 0
      t.integer  "Penalties",         limit: 1, default: 0
      t.integer  "PenaltyYards",      limit: 4, default: 0
      t.integer  "Fumbles",           limit: 1, default: 0
      t.integer  "FumblesLost",       limit: 1, default: 0
      t.datetime "CreatedDate"
      t.datetime "ModifiedDate"
    end

    add_index "TeamStats_Football", ["GameID"], name: "GameID", using: :btree
    add_index "TeamStats_Football", ["LeagueID"], name: "LeagueID", using: :btree
    add_index "TeamStats_Football", ["TeamID"], name: "TeamID", using: :btree

    create_table "TeamStats_Hockey", id: false, force: :cascade do |t|
      t.integer  "GameID",         limit: 11,             null: false
      t.integer  "LeagueID",       limit: 11,             null: false
      t.integer  "TeamID",         limit: 4,             null: false
      t.integer  "Period_1",       limit: 1, default: 0
      t.integer  "Period_2",       limit: 1, default: 0
      t.integer  "Period_3",       limit: 1, default: 0
      t.integer  "Overtime_1",     limit: 1, default: 0
      t.integer  "Overtime_2",     limit: 1, default: 0
      t.integer  "Shootout",       limit: 1, default: 0
      t.integer  "FinalScore",     limit: 1, default: 0
      t.integer  "TotalShots",     limit: 1, default: 0
      t.integer  "Shots_1",        limit: 1, default: 0
      t.integer  "Shots_2",        limit: 1, default: 0
      t.integer  "Shots_3",        limit: 1, default: 0
      t.integer  "PowerPlays",     limit: 1, default: 0
      t.integer  "PPConverted",    limit: 1, default: 0
      t.integer  "PPPercent",      limit: 1, default: 0
      t.integer  "PenaltyMinutes", limit: 1, default: 0
      t.integer  "FaceoffsWon",    limit: 1, default: 0
      t.integer  "FaceoffPercent", limit: 1, default: 0
      t.integer  "Hits",           limit: 1, default: 0
      t.integer  "Blocks",         limit: 1, default: 0
      t.datetime "CreatedDate"
      t.datetime "ModifiedDate"
    end

    add_index "TeamStats_Hockey", ["GameID"], name: "GameID", using: :btree
    add_index "TeamStats_Hockey", ["LeagueID"], name: "LeagueID", using: :btree
    add_index "TeamStats_Hockey", ["TeamID"], name: "TeamID", using: :btree

    create_table "TeamStats_Soccer", id: false, force: :cascade do |t|
      t.integer  "GameID",       limit: 11,             null: false
      t.integer  "LeagueID",     limit: 11,             null: false
      t.integer  "TeamID",       limit: 4,             null: false
      t.integer  "FinalScore",   limit: 1, default: 0
      t.integer  "Possession",   limit: 1, default: 0
      t.integer  "Tackles",      limit: 1, default: 0
      t.integer  "ShotsTotal",   limit: 1, default: 0
      t.integer  "ShotsOnGoal",  limit: 1, default: 0
      t.integer  "Corners",      limit: 1, default: 0
      t.integer  "Saves",        limit: 1, default: 0
      t.integer  "Offsides",     limit: 1, default: 0
      t.integer  "Fouls",        limit: 1, default: 0
      t.integer  "YellowCards",  limit: 1, default: 0
      t.integer  "RedCards",     limit: 1, default: 0
      t.datetime "CreatedDate"
      t.datetime "ModifiedDate"
    end

    add_index "TeamStats_Soccer", ["GameID"], name: "GameID", using: :btree
    add_index "TeamStats_Soccer", ["LeagueID"], name: "LeagueID", using: :btree
    add_index "TeamStats_Soccer", ["TeamID"], name: "TeamID", using: :btree

    create_table "Teams", primary_key: "TeamID", force: :cascade do |t|
      t.integer  "LeagueID",     limit: 11
      t.string   "TeamPrefix",   limit: 10
      t.string   "TeamName",     limit: 50
      t.string   "TeamCity",     limit: 50
      t.string   "ESPNUrl",      limit: 150
      t.datetime "CreatedDate",              null: false
      t.datetime "ModifiedDate",             null: false
    end

    add_index "Teams", ["LeagueID"], name: "LeagueID", using: :btree

    add_foreign_key "PlayerStats_Baseball", "Games", column: "GameID", primary_key: "GameID", name: "GameIDBaseball"
    add_foreign_key "PlayerStats_Baseball", "Leagues", column: "LeagueID", primary_key: "LeagueID", name: "LeagueIDBaseball"
    add_foreign_key "PlayerStats_Baseball", "Players", column: "PlayerID", primary_key: "PlayerID", name: "PlayerIDBaseball"
    add_foreign_key "PlayerStats_Baseball", "Teams", column: "TeamID", primary_key: "TeamID", name: "TeamIDBaseball"
    add_foreign_key "PlayerStats_Basketball", "Leagues", column: "LeagueID", primary_key: "LeagueID", name: "LeagueIDBasketball"
    add_foreign_key "PlayerStats_Basketball", "Players", column: "PlayerID", primary_key: "PlayerID", name: "PlayerIDBasketball"
    add_foreign_key "PlayerStats_Basketball", "Teams", column: "TeamID", primary_key: "TeamID", name: "TeamIDBasketball"
    add_foreign_key "PlayerStats_Football", "Leagues", column: "LeagueID", primary_key: "LeagueID", name: "LeagueIDFootball"
    add_foreign_key "PlayerStats_Football", "Players", column: "PlayerID", primary_key: "PlayerID", name: "PlayerIDFootball"
    add_foreign_key "PlayerStats_Football", "Teams", column: "TeamID", primary_key: "TeamID", name: "TeamIDFootball"
    add_foreign_key "PlayerStats_Hockey", "Leagues", column: "LeagueID", primary_key: "LeagueID", name: "LeagueIDHockey"
    add_foreign_key "PlayerStats_Hockey", "Players", column: "PlayerID", primary_key: "PlayerID", name: "PlayerIDHockey"
    add_foreign_key "PlayerStats_Hockey", "Teams", column: "TeamID", primary_key: "TeamID", name: "TeamIDHockey"
    add_foreign_key "PlayerStats_Soccer", "Leagues", column: "LeagueID", primary_key: "LeagueID", name: "LeagueIDSoccer"
    add_foreign_key "PlayerStats_Soccer", "Players", column: "PlayerID", primary_key: "PlayerID", name: "PlayerIDSoccer"
    add_foreign_key "PlayerStats_Soccer", "Teams", column: "TeamID", primary_key: "TeamID", name: "TeamIDSoccer"
    add_foreign_key "TeamStats_Baseball", "Leagues", column: "LeagueID", primary_key: "LeagueID", name: "LeagueIDBaseballTeam"
    add_foreign_key "TeamStats_Baseball", "Teams", column: "TeamID", primary_key: "TeamID", name: "TeamIDBaseballTeam"
    add_foreign_key "TeamStats_Basketball", "Leagues", column: "LeagueID", primary_key: "LeagueID", name: "LeagueIDBasketballTeam"
    add_foreign_key "TeamStats_Basketball", "Teams", column: "TeamID", primary_key: "TeamID", name: "TeamIDBasketballTeam"
    add_foreign_key "TeamStats_Football", "Leagues", column: "LeagueID", primary_key: "LeagueID", name: "LeagueIDFootballTeam"
    add_foreign_key "TeamStats_Football", "Teams", column: "TeamID", primary_key: "TeamID", name: "TeamIDFootballTeam"
    add_foreign_key "TeamStats_Hockey", "Leagues", column: "LeagueID", primary_key: "LeagueID", name: "LeagueIDHockeyTeam"
    add_foreign_key "TeamStats_Hockey", "Teams", column: "TeamID", primary_key: "TeamID", name: "TeamIDHockeyTeam"
    add_foreign_key "TeamStats_Soccer", "Leagues", column: "LeagueID", primary_key: "LeagueID", name: "LeagueIDSoccerTeam"
    add_foreign_key "TeamStats_Soccer", "Teams", column: "TeamID", primary_key: "TeamID", name: "TeamIDSoccerTeam"
  end
end
