require_relative '../btop'
require 'test/unit'

class BtopTest < Test::Unit::TestCase
  def btop
    Btop.new!
  end

  def test_find_quarter
    current_time = Time.new(2014, 01, 01, 0, 59, 59) # 4th quarter

    expected = btop.find_quarter(current_time)

    assert_equal expected, 4
  end
end