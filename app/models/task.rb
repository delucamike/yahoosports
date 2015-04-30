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

class Task < ActiveRecord::Base
  enum status: { stopped: 0, running: 1 }

  has_many :logs, dependent: :destroy, class_name: 'TaskLog'

  validates :interval, numericality: { only_integer: true, greater_than: 0 }
  validates :league_name, presence: true

  before_destroy :stop!

  def stop!
    TaskRunnerService.new(self).stop!
  end

  def run!
    TaskRunnerService.new(self).run!
  end
end
