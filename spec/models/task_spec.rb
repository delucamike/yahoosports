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

require 'rails_helper'

RSpec.describe Task, type: :model do
  it { is_expected.to have_many(:logs).dependent(:destroy).class_name('TaskLog') }

  it { is_expected.to validate_numericality_of(:interval).only_integer.is_greater_than(0) }
  it { is_expected.to validate_presence_of(:league_name) }
end
