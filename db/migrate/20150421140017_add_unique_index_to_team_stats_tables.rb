class AddUniqueIndexToTeamStatsTables < ActiveRecord::Migration
  def change
    add_index 'TeamStats_Baseball', ['GameID', 'TeamID'], unique: true
    add_index 'TeamStats_Basketball', ['GameID', 'TeamID'], unique: true
    add_index 'TeamStats_Football', ['GameID', 'TeamID'], unique: true
    add_index 'TeamStats_Hockey', ['GameID', 'TeamID'], unique: true
    add_index 'TeamStats_Soccer', ['GameID', 'TeamID'], unique: true
  end
end
