class CreateTasks < ActiveRecord::Migration
  def change
    create_table :tasks do |t|
      t.string :name
      t.integer :interval, default: 60
      t.integer :pid
      t.string :league_name, default: 'NBA'
      t.string :scraper, default: 'SportsScraper'
      t.integer :status, default: 0

      t.timestamps null: false
    end
  end
end
