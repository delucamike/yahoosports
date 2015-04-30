class CreateTaskLogs < ActiveRecord::Migration
  def change
    create_table :task_logs do |t|
      t.references :task, index: true

      t.datetime :start_time
      t.datetime :end_time
      t.integer :records_updated, default: 0
      t.integer :records_inserted, default: 0
      t.integer :games_in_progress
      t.string :league_name

      t.timestamps null: false
    end

    add_foreign_key :task_logs, :tasks
  end
end
