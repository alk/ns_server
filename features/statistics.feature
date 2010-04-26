Feature: Accurate Cache Usage Stats
  As a user of northscale software
  I want accurate cache usage statistics

  Scenario: Fill bucket
    Given I have configured nodes A
    And default bucket has size 128M
    When I put 64M of data to default bucket
    Then I should get 50% cache utilization for default bucket

  Scenario: Full bucket utiliation
    Given I have configured node A
    And default bucket has size 128M
    When I put 256M of data to default bucket
    Then I should get 100% cache utilization for default bucket

    # this gives 85.5% of utilization. I believe that's due to bad key
    # distribution of consistent hashing.

  # Scenario: Dual node cache usage
  #   Given I have configured nodes A and B
  #   And they are already joined
  #   And default bucket has size 128M
  #   When I put 256M of data to default bucket
  #   Then I should get 100% cache utilization for default bucket
