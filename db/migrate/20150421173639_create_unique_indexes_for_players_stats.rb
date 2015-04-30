class CreateUniqueIndexesForPlayersStats < ActiveRecord::Migration
  def change
    add_index 'PlayerStats_Baseball'  , ['GameID', 'TeamID', 'PlayerID'] , unique: true
    add_index 'PlayerStats_Basketball', ['GameID', 'TeamID', 'PlayerID'] , unique: true
    add_index 'PlayerStats_Football'  , ['GameID', 'TeamID', 'PlayerID'] , unique: true
    add_index 'PlayerStats_Hockey'    , ['GameID', 'TeamID', 'PlayerID'] , unique: true
    add_index 'PlayerStats_Soccer'    , ['GameID', 'TeamID', 'PlayerID'] , unique: true

    add_index 'PlayerStats_Racing'    , ['GameID', 'PlayerID'], unique: true
    add_index 'PlayerStats_Golf'      , ['GameID', 'PlayerID'], unique: true
  end
end
