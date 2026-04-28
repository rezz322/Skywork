package main

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"
	// "strings"
	"time"

	_ "github.com/lib/pq"
)

const (
	CryptoPayAPI = "https://testnet-pay.crypt.bot/api/createInvoice"
)

// Структуры для запроса от Бота
type CreateInvoiceRequest struct {
	PriceAmount      float64 `json:"price_amount"`
	Asset            string  `json:"asset"` // Валюта (USDT, TON, TRX...)
	OrderID          string  `json:"order_id"`
	OrderDescription string  `json:"order_description"`
}

// Структуры для Crypto Pay API
type CryptoPayCreateRequest struct {
	Asset          string `json:"asset"`
	Amount         string `json:"amount"`
	Description    string `json:"description,omitempty"`
	Payload        string `json:"payload,omitempty"`
	AllowAnonymous bool   `json:"allow_anonymous"`
}

type CryptoPayResponse struct {
	Ok     bool `json:"ok"`
	Result struct {
		InvoiceID int    `json:"invoice_id"`
		PayURL    string `json:"pay_url"`
		Status    string `json:"status"`
	} `json:"result"`
	ErrorCode   int    `json:"error_code,omitempty"`
	Description string `json:"description,omitempty"`
}

type ExchangeRate struct {
	IsValid bool   `json:"is_valid"`
	Source  string `json:"source"`
	Target  string `json:"target"`
	Rate    string `json:"rate"`
}

type ExchangeRatesResponse struct {
	Ok     bool           `json:"ok"`
	Result []ExchangeRate `json:"result"`
}

// Структуры для Webhook
type CryptoPayWebhook struct {
	UpdateID   int    `json:"update_id"`
	UpdateType string `json:"update_type"`
	Payload    struct {
		InvoiceID int     `json:"invoice_id"`
		Status    string  `json:"status"`
		Payload   string  `json:"payload"`
		Amount    string  `json:"amount"`
		Asset     string  `json:"asset"`
		PaidAt    string  `json:"paid_at"`
	} `json:"payload"`
}

var db *sql.DB

func initDB() {
	dbHost := os.Getenv("DB_HOST")
	dbPort := os.Getenv("DB_PORT")
	dbUser := os.Getenv("DB_USER")
	dbPass := os.Getenv("DB_PASSWORD")
	dbName := os.Getenv("DB_NAME")

	dsn := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=disable",
		dbHost, dbPort, dbUser, dbPass, dbName)

	var err error
	db, err = sql.Open("postgres", dsn)
	if err != nil {
		log.Fatalf("Error opening database: %v", err)
	}

	for i := 0; i < 10; i++ {
		err = db.Ping()
		if err == nil {
			log.Println("Successfully connected to database")
			return
		}
		log.Printf("Waiting for database... (%d/10)", i+1)
		time.Sleep(2 * time.Second)
	}
	log.Fatalf("Could not connect to database: %v", err)
}

func getExchangeRate(asset, target string) (float64, error) {
	apiToken := os.Getenv("CRYPTO_PAY_TOKEN")
	url := "https://testnet-pay.crypt.bot/api/getExchangeRates"
	req, _ := http.NewRequest("GET", url, nil)
	req.Header.Set("Crypto-Pay-API-Token", apiToken)

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()

	var erResp ExchangeRatesResponse
	if err := json.NewDecoder(resp.Body).Decode(&erResp); err != nil {
		return 0, err
	}

	for _, r := range erResp.Result {
		if r.Source == asset && r.Target == target {
			rate, _ := strconv.ParseFloat(r.Rate, 64)
			return rate, nil
		}
	}
	return 0, fmt.Errorf("rate not found")
}

func handleCreateInvoice(w http.ResponseWriter, r *http.Request) {
	apiToken := os.Getenv("CRYPTO_PAY_TOKEN")
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req CreateInvoiceRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	log.Printf("Creating CryptoPay invoice for OrderID: %s, USD Amount: %f, Asset: %s", req.OrderID, req.PriceAmount, req.Asset)

	asset := req.Asset
	if asset == "" {
		asset = "USDT"
	}

	// Получаем курс обмена
	cryptoAmount := req.PriceAmount
	if asset != "USDT" && asset != "USDC" {
		rate, err := getExchangeRate(asset, "USD")
		if err != nil {
			log.Printf("Error getting exchange rate: %v. Using 1:1 fallback.", err)
		} else if rate > 0 {
			cryptoAmount = req.PriceAmount / rate
			log.Printf("Converted %f USD to %f %s (Rate: %f)", req.PriceAmount, cryptoAmount, asset, rate)
		}
	}

	cpReq := CryptoPayCreateRequest{
		Asset:          asset,
		Amount:         fmt.Sprintf("%.6f", cryptoAmount),
		Description:    req.OrderDescription,
		Payload:        fmt.Sprintf("%s:%.2f", req.OrderID, req.PriceAmount), // Сохраняем USD сумму в payload
		AllowAnonymous: false,
	}

	jsonData, _ := json.Marshal(cpReq)
	client := &http.Client{}
	
	apiReq, _ := http.NewRequest("POST", CryptoPayAPI, bytes.NewBuffer(jsonData))
	apiReq.Header.Set("Crypto-Pay-API-Token", apiToken)
	apiReq.Header.Set("Content-Type", "application/json")

	resp, err := client.Do(apiReq)
	if err != nil {
		log.Printf("Error calling CryptoPay: %v", err)
		http.Error(w, "Failed to connect to CryptoPay", http.StatusInternalServerError)
		return
	}
	defer resp.Body.Close()

	var cpResp CryptoPayResponse
	body, _ := io.ReadAll(resp.Body)
	if err := json.Unmarshal(body, &cpResp); err != nil {
		log.Printf("Error decoding CryptoPay response: %v, Body: %s", err, string(body))
		http.Error(w, "Invalid response from CryptoPay", http.StatusInternalServerError)
		return
	}

	if !cpResp.Ok {
		log.Printf("CryptoPay Error: %d - %s", cpResp.ErrorCode, cpResp.Description)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		w.Write(body)
		return
	}

	result := map[string]interface{}{
		"invoice_url": cpResp.Result.PayURL,
		"invoice_id":  cpResp.Result.InvoiceID,
		"status":      cpResp.Result.Status,
	}
	
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(result)
}

func main() {
	// apiToken := os.Getenv("CRYPTO_PAY_TOKEN")

	initDB()
	defer db.Close()

	// Endpoints disabled for now
	/*
	http.HandleFunc("/invoice/create", handleCreateInvoice)

	http.HandleFunc("/callback", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}

		body, err := io.ReadAll(r.Body)
		if err != nil {
			log.Printf("Error reading body: %v", err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		signature := r.Header.Get("crypto-pay-api-signature")
		if !verifySignature(body, signature, apiToken) {
			log.Println("Invalid CryptoPay signature received")
			w.WriteHeader(http.StatusForbidden)
			return
		}

		var webhook CryptoPayWebhook
		if err := json.Unmarshal(body, &webhook); err != nil {
			log.Printf("Error parsing webhook JSON: %v", err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		if webhook.UpdateType == "invoice_paid" {
			payloadParts := strings.Split(webhook.Payload.Payload, ":")
			orderID := payloadParts[0]
			usdAmount := 10.0 // Default fallback
			if len(payloadParts) > 1 {
				usdAmount, _ = strconv.ParseFloat(payloadParts[1], 64)
			}
			
			amountStr := webhook.Payload.Amount
			cryptoAmount, _ := strconv.ParseFloat(amountStr, 64)

			log.Printf("Payment confirmed! UserID=%s, USD=%.2f, Crypto=%f %s", orderID, usdAmount, cryptoAmount, webhook.Payload.Asset)
			
			months := int(usdAmount / 10.0)
			if months == 0 {
				months = 1
			}
			interval := fmt.Sprintf("%d months", months)

			// Обновляем таблицу subscriptions (с продлением, если уже есть активная)
			query := fmt.Sprintf(`
				INSERT INTO subscriptions (user_id, status, current_period_start, current_period_end, updated_at)
				VALUES ($1, 'active', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP + INTERVAL '%%s', CURRENT_TIMESTAMP)
				ON CONFLICT (user_id) DO UPDATE SET
					status = 'active',
					current_period_end = GREATEST(COALESCE(subscriptions.current_period_end, CURRENT_TIMESTAMP), CURRENT_TIMESTAMP) + INTERVAL '%%s',
					updated_at = CURRENT_TIMESTAMP
			`)
			
			_, err := db.Exec(fmt.Sprintf(query, interval, interval), orderID)
			if err != nil {
				log.Printf("Error updating database for UserID %s: %v", orderID, err)
			} else {
				log.Printf("Database updated successfully for UserID %s", orderID)
				// Отправляем уведомление пользователю в бот
				msg := fmt.Sprintf(
					"🚀 **Успішна оплата!**\n\n"+
					"💎 Ваша підписка була успешно активована.\n\n"+
					"📊 **Деталі транзакції:**\n"+
					"▫️ Сума: `%.2f USD` (`%f %s`)\n"+
					"▫️ Період: **+%d міс.**\n\n"+
					"⚡️ Тепер вам доступний повний функціонал пошуку без обмежень.\n\n"+
					"🤝 Дякуємо, що обрали наш сервіс!",
					usdAmount, cryptoAmount, webhook.Payload.Asset, months)
				sendTelegramNotification(orderID, msg)
			}
		}
		
		w.WriteHeader(http.StatusOK)
	})
	*/

	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	log.Printf("PayService (Crypto Bot) started on port %s", port)
	if err := http.ListenAndServe(":"+port, nil); err != nil {
		log.Fatal(err)
	}
}

// Отправка сообщения пользователю через Telegram Bot API
func sendTelegramNotification(userID string, text string) {
	botToken := os.Getenv("TELEGRAM_BOT_TOKEN")
	if botToken == "" {
		log.Println("TELEGRAM_BOT_TOKEN not found, cannot send notification")
		return
	}
	url := fmt.Sprintf("https://api.telegram.org/bot%s/sendMessage", botToken)
	
	payload := map[string]interface{}{
		"chat_id":    userID,
		"text":       text,
		"parse_mode": "Markdown",
	}
	
	jsonData, _ := json.Marshal(payload)
	resp, err := http.Post(url, "application/json", bytes.NewBuffer(jsonData))
	if err != nil {
		log.Printf("Error sending Telegram notification: %v", err)
		return
	}
	defer resp.Body.Close()
	log.Printf("Telegram notification sent to %s, Status: %s", userID, resp.Status)
}

func verifySignature(payload []byte, signature string, token string) bool {
	hToken := sha256.New()
	hToken.Write([]byte(token))
	tokenHash := hToken.Sum(nil)

	h := hmac.New(sha256.New, tokenHash)
	h.Write(payload)
	expectedSignature := hex.EncodeToString(h.Sum(nil))
	
	return expectedSignature == signature
}
