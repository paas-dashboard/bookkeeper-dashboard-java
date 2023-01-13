# bookkeeper-dashboard
bookkeeper dashboard for fun.
## backend api command
### ledger
#### create ledger
```bash
curl -X PUT -H "Content-Type: application/json" http://localhost:10007/api/bookkeeper/ledgers
```
### ledger content
#### put ledger content
```bash
curl -X PUT -H "Content-Type: application/json" http://localhost:10007/api/bookkeeper/ledgers/1/entries -d '{"content":"test"}'
```
