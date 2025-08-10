# HubSpot Data Exporter

A Python-based tool for securely and efficiently exporting **all** HubSpot CRM objects into local CSV files using the official HubSpot API v3. Designed for data analysts, marketers, and developers who need offline access to HubSpot data for reporting, migration, or backup purposes.

---

## 📌 Features

- **Comprehensive Coverage**  
  Exports 20+ HubSpot CRM objects, including Contacts, Companies, Deals, Tickets, Products, Quotes, and more.

- **Batch Processing**  
  Automatically handles pagination to fetch **all** records (not just the first 1000).

- **Secure Authentication**  
  Uses **Bearer Token** authentication via environment variables (`HUBSPOT_API_KEY`).

- **Clean Output**  
  Saves data as **UTF-8 encoded CSV files** with flattened JSON structures for easy analysis.

- **Zero Dependencies (beyond standard libraries)**  
  Uses only `requests` and `pandas`, which are lightweight and widely supported.

---

## 🚀 Quick Start

### 1. Clone the Repository
```bash
git clone https://github.com/your-org/HubSpot.git
cd HubSpot
```

### 2. Install Dependencies
```bash
pip install requests pandas
```

### 3. Set Up API Key
Create a `.env` file in the root directory and add your **HubSpot Private App Token**:
```bash
HUBSPOT_API_KEY=your_private_app_token_here
```

> 🔐 **Security Tip:** Never commit your API key to version control. The `.gitignore` file already excludes `.env`.

### 4. Run the Exporter
```bash
python main.py
```

### 5. Access Your Data
All CSV files will be saved in the `hubspot_data/` directory:
```
hubspot_data/
├── contacts.csv
├── companies.csv
├── deals.csv
├── tickets.csv
├── products.csv
├── quotes.csv
└── ... (other objects)
```

---

## 📊 Supported HubSpot Objects

| Object Name       | API Endpoint                          | Description                          |
|-------------------|----------------------------------------|--------------------------------------|
| `carts`           | `crm/v3/objects/carts`                | Shopping cart data                   |
| `companies`       | `crm/v3/objects/companies`            | Company records                      |
| `contacts`        | `crm/v3/objects/contacts`             | Contact records                      |
| `deals`           | `crm/v3/objects/deals`                | Deal/opportunity records             |
| `discounts`       | `crm/v3/objects/discounts`            | Discount records                     |
| `fees`            | `crm/v3/objects/fees`                 | Fee records                          |
| `goals`           | `crm/v3/objects/goals`                | Goal tracking records                |
| `invoices`        | `crm/v3/objects/invoices`             | Invoice records                      |
| `leads`           | `crm/v3/objects/leads`                | Lead records                         |
| `line_items`      | `crm/v3/objects/line_items`           | Line item records                    |
| `orders`          | `crm/v3/objects/orders`               | Order records                        |
| `partner_clients` | `crm/v3/objects/partner_clients`      | Partner client records               |
| `partner_services`| `crm/v3/objects/partner_services`     | Partner service records              |
| `payments`        | `crm/v3/objects/payments`             | Payment records                      |
| `products`        | `crm/v3/objects/products`             | Product records                      |
| `quotes`          | `crm/v3/objects/quotes`               | Quote records                        |
| `taxes`           | `crm/v3/objects/taxes`                | Tax records                          |
| `tickets`         | `crm/v3/objects/tickets`              | Support ticket records               |

---

## ⚙️ Configuration

### Environment Variables
| Variable         | Required | Description                          |
|------------------|----------|--------------------------------------|
| `HUBSPOT_API_KEY`| ✅       | Your HubSpot Private App Token       |

### Customization
You can modify the `OBJECTS` dictionary in `main.py` to:
- Add new objects (e.g., custom objects)
- Remove objects you don't need
- Change API endpoints (if using non-standard ones)

---

## 🛠️ Development

### Project Structure
```
hubspot-data-exporter/
├── main.py          # Main script
├── README.md        # This file
├── .gitignore       # Git ignore rules
├── .env             # Environment variables (not tracked)
└── hubspot_data/    # Output directory (created automatically)
```

### Error Handling
The script includes:
- **HTTP error handling** (`response.raise_for_status()`)
- **Timeout protection** (60s per request)
- **Graceful pagination** (handles API limits)

---

## 📈 Performance Notes

- **Rate Limits:** HubSpot allows 100 requests/10 seconds for most endpoints. The script respects this by processing sequentially.
- **Memory Usage:** Uses `pandas` for efficient JSON-to-CSV conversion. For very large datasets (>1M records), consider streaming to disk.
- **Parallel Processing:** Not implemented to avoid rate limit issues. For faster exports, implement exponential backoff + threading.

---

## 🔍 Troubleshooting

| Issue                          | Solution                                                                 |
|--------------------------------|--------------------------------------------------------------------------|
| `401 Unauthorized`             | Check if `HUBSPOT_API_KEY` is correct and has required scopes             |
| `403 Forbidden`                | Ensure your Private App has access to the requested objects               |
| `429 Rate Limit`               | Add `time.sleep()` between requests or implement exponential backoff      |
| Empty CSV files                | Verify the object exists in your HubSpot account and has data           |

---

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/new-object`)
3. Commit your changes (`git commit -am 'Add support for custom objects'`)
4. Push to the branch (`git push origin feature/new-object`)
5. Open a Pull Request

---

## 📄 License

This project is licensed under the **MIT License** - see the [LICENSE](LICENSE) file for details.

---

## 🙋‍♂️ Support

- **HubSpot API Docs:** [https://developers.hubspot.com/docs/api/crm/understanding-the-crm](https://developers.hubspot.com/docs/api/crm/understanding-the-crm)
- **Issues:** Report bugs via [GitHub Issues](https://github.com/your-org/hubspot-data-exporter/issues)

---

**Happy Exporting!** 🎉
