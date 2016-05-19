from scipy.stats.stats import pearsonr
import csv

f = open('../../msr-results.csv')
csv_f = csv.reader(f)

commits = []
pull_requests = []

for row in csv_f:
    commits.append(row[0])
    pull_requests.append(row[1])

commits.pop(0)
pull_requests.pop(0)

commits = map(int, commits)
pull_requests = map(int, pull_requests)

print commits
print pull_requests


correlation_coefficient, p_value = pearsonr(commits, pull_requests)

print correlation_coefficient
print p_value
