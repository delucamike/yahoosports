# == Schema Information
#
# Table name: task_logs
#
#  id                :integer          not null, primary key
#  task_id           :integer
#  start_time        :datetime
#  end_time          :datetime
#  records_updated   :integer          default(0)
#  records_inserted  :integer          default(0)
#  games_in_progress :integer          default(0)
#  league_name       :string(255)
#  created_at        :datetime         not null
#  updated_at        :datetime         not null
#
# Indexes
#
#  index_task_logs_on_task_id  (task_id)
#

FactoryGirl.define do
  factory :task_log do
    task
  end
end
