# == Schema Information
#
# Table name: tasks
#
#  id          :integer          not null, primary key
#  name        :string(255)
#  interval    :integer          default(60)
#  pid         :integer
#  league_name :string(255)      default("NBA")
#  scraper     :string(255)      default("SportsScraper")
#  status      :integer          default(0)
#  created_at  :datetime         not null
#  updated_at  :datetime         not null
#

FactoryGirl.define do
  factory :task do
    sequence(:name) { |n| "Task#{n}" }
    interval 60
    league_name 'NBA'
  end
end
