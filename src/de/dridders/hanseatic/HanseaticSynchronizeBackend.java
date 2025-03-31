package de.dridders.hanseatic;

import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.rmi.RemoteException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;

import javax.annotation.Resource;

import org.htmlunit.CookieManager;
import org.htmlunit.HttpMethod;
import org.htmlunit.Page;
import org.htmlunit.ProxyConfig;
import org.htmlunit.WebClient;
import org.htmlunit.WebRequest;
import org.htmlunit.html.HtmlPage;
import org.json.JSONArray;
import org.json.JSONObject;

import de.willuhn.annotation.Lifecycle;
import de.willuhn.annotation.Lifecycle.Type;
import de.willuhn.datasource.rmi.DBIterator;
import de.willuhn.jameica.hbci.Settings;
import de.willuhn.jameica.hbci.SynchronizeOptions;
import de.willuhn.jameica.hbci.messaging.ImportMessage;
import de.willuhn.jameica.hbci.messaging.SaldoMessage;
import de.willuhn.jameica.hbci.rmi.Konto;
import de.willuhn.jameica.hbci.rmi.Umsatz;
import de.willuhn.jameica.hbci.synchronize.AbstractSynchronizeBackend;
import de.willuhn.jameica.hbci.synchronize.SynchronizeBackend;
import de.willuhn.jameica.hbci.synchronize.SynchronizeEngine;
import de.willuhn.jameica.hbci.synchronize.jobs.SynchronizeJob;
import de.willuhn.jameica.hbci.synchronize.jobs.SynchronizeJobKontoauszug;
import de.willuhn.jameica.security.Wallet;
import de.willuhn.jameica.system.Application;
import de.willuhn.logging.Logger;
import de.willuhn.util.ApplicationException;
import de.willuhn.util.ProgressMonitor;

@Lifecycle(Type.CONTEXT)
public class HanseaticSynchronizeBackend extends AbstractSynchronizeBackend<HanseaticSynchronizeJobProvider>{
    protected static Hashtable<String,String> passwortHashtable = new Hashtable<String,String>();

    @Resource
    private SynchronizeEngine engine = null;

    @Override
    public String getName() {
        return "HanseaticBank";
    }

    @Override
    protected Class<HanseaticSynchronizeJobProvider> getJobProviderInterface() {
        return HanseaticSynchronizeJobProvider.class;
    }

    @Override
    protected de.willuhn.jameica.hbci.synchronize.AbstractSynchronizeBackend.JobGroup createJobGroup(Konto k) {
        return new HanseaticJobGroup(k);
    }
    
    public List<Konto> getSynchronizeKonten(Konto k)
    {
        List<Konto> list = super.getSynchronizeKonten(k);
        List<Konto> result = new ArrayList<Konto>();
        
        // Wir wollen nur die Offline-Konten und jene, bei denen Scripting explizit konfiguriert ist
        for (Konto konto:list)
        {
            if (konto != null)
            {
            	SynchronizeBackend backend = engine.getBackend(konto);
            	if (backend != null && backend.equals(this))
            	{
	                result.add(konto);
            	}
            }
        }
        
        return result;
    }
    
    protected class HanseaticJobGroup extends JobGroup
    {
        protected HanseaticJobGroup(Konto k) {
            super(k);
        }
        
        protected void sync() throws Exception
        {
            Konto konto = getKonto();
            ProgressMonitor monitor = getCurrentSession().getProgressMonitor();
            SynchronizeOptions options = new SynchronizeOptions(konto);

            Boolean forceSaldo  = (Boolean)((SynchronizeJob)jobs.get(0)).getContext(SynchronizeJobKontoauszug.CTX_FORCE_SALDO);
            Boolean forceUmsatz = (Boolean)((SynchronizeJob)jobs.get(0)).getContext(SynchronizeJobKontoauszug.CTX_FORCE_UMSATZ);
            
            Boolean fetchSaldo = options.getSyncSaldo() || forceSaldo;
            Boolean fetchUmsatz = options.getSyncKontoauszuege() || forceUmsatz;
            Logger.info("Hanseatci: Neue Synchronisierung wurde erkannt, mit folgenden Einstellungen: ");
            Logger.info("Hanseatic: forceSaldo: " + forceSaldo + ", forceUmsatz: " + forceUmsatz + ", fetchSaldo: " + fetchSaldo + ", fetchUmsatz: " + fetchUmsatz);
            
            options.setAutoSaldo(false);
            
            WebClient webClient = new WebClient(new org.htmlunit.BrowserVersion.BrowserVersionBuilder(org.htmlunit.BrowserVersion.FIREFOX)
                    .setAcceptLanguageHeader("de-DE")
                    .setSecClientHintUserAgentHeader(null)
                    .setSecClientHintUserAgentPlatformHeader(null)
                    .setApplicationCodeName(null)
                    .setCssAcceptHeader(null)
                    .setHtmlAcceptHeader(null)
                    .setImgAcceptHeader(null)
                    .setScriptAcceptHeader(null)
                    .setXmlHttpRequestAcceptHeader(null)
                    .build());
            CookieManager cookieCache;
            DBIterator<Umsatz> umsaetze=null;

            monitor.setPercentComplete(0);
            monitor.log("Synchronisiere Konto: " + konto.getLongName());

            // Ausgabe der Versionsnummer des Scripts, ist oben unter Konfiguration einzustellen
            Logger.info("Hanseatic: Version 0.0.1 wurde gestartet ...");
            monitor.log("Hanseatic V0.0.1 wurde gestartet ...");
            monitor.log("******************************************************************************************************************\n");

            if (!fetchSaldo && !fetchUmsatz) {
                Logger.warn("Hanseatic: Neuer Sync wird nicht ausgef\u00fcrt da die Option 'Saldo aktualisieren' und 'Kontoausz\u00fcge (Ums\u00e4tze) abrufen' deaktiviert sind. Nichts zu tun");
                monitor.log("Neuer Sync wird nicht ausgef\u00fcrt da die Option 'Saldo aktualisieren' und 'Kontoausz\u00fcge (Ums\u00e4tze) abrufen' deaktiviert sind. Nichts zu tun");
            };
            
            Logger.debug("Hanseatic: Ums\u00e4tze von Hibiscus f\u00fcr Doppelbuchung-Checks holen ...");
            umsaetze = konto.getUmsaetze();
            Logger.debug("Hanseatic: Alle Buchungen aus dem Cache: "+umsaetze);
            monitor.setPercentComplete(1);
            
            webClient.getOptions().setUseInsecureSSL(false);
            webClient.getOptions().setRedirectEnabled(true);
            webClient.getOptions().setJavaScriptEnabled(false);
            webClient.getOptions().setThrowExceptionOnScriptError(false);
            webClient.getOptions().setThrowExceptionOnFailingStatusCode(false);
            webClient.getOptions().setCssEnabled(false);

            //java.util.logging.Logger.getLogger("com.gargoylesoftware").setLevel(java.util.logging.Level.OFF); 
            //Logger.debug("Hanseatic: Logging-Einstellung von 'com.gargoylesoftware' ist "+java.util.logging.Logger.getLogger("com.gargoylesoftware").getLevel());

            Logger.debug("Hanseatic: es wird auf eine Proxy-Konfiguration gepr\u00FCft ...");
            Boolean useSystemProxy = Application.getConfig().getUseSystemProxy();
            String httpProxyHost = Application.getConfig().getProxyHost();
            Integer httpProxyPort = Application.getConfig().getProxyPort();
            String httpsProxyHost = Application.getConfig().getHttpsProxyHost();
            Integer httpsProxyPort = Application.getConfig().getHttpsProxyPort();
            ProxyConfig proxyConfig = null;

            Logger.info("Hanseatic: Proxy Einstellungen setzten ...");
            monitor.log("Proxy Einstellungen setzten ...");
            
            Logger.debug("Hanseatic: Jameica nutzt den System-Proxy: " + useSystemProxy);
            Logger.debug("Hanseatic: HTTP-Proxy Host von Jameica ist: " + httpProxyHost);
            Logger.debug("Hanseatic: HTTP-Proxy Port von Jameica ist: " + httpProxyPort);
            Logger.debug("Hanseatic: HTTPS-Proxy Host von Jameica ist: " + httpsProxyHost);
            Logger.debug("Hanseatic: HTTPS-Proxy Port von Jameica ist: " + httpsProxyPort);

            if (useSystemProxy == true) {
                java.lang.System.setProperty("java.net.useSystemProxies", "true");
                String SysProxyInfoHTTP = new java.lang.String(java.net.ProxySelector.getDefault().select(new java.net.URI("http://www.java.de")).get(0).toString());
                String SysProxyInfoHTTPS = new java.lang.String(java.net.ProxySelector.getDefault().select(new java.net.URI("https://www.mydrive.ch")).get(0).toString()); 
                int SysProxyFehler = 0;
                
                if ((SysProxyInfoHTTP.equals("DIRECT")) && (SysProxyInfoHTTPS.equals("DIRECT")))
                {
                    monitor.log("Info-Warnung: Systemproxy-Einstellungen verwenden ist in Jameica eingestellt, es ist aber kein Proxy im System eingetragen!");
                    Logger.info("Hanseatic: Systemproxy-Einstellungen verwenden ist in Jameica eingestellt, es ist aber kein Proxy im System eingetragen!");
                    proxyConfig = new ProxyConfig();
                } 
                else 
                {
                    String SysHttpsProxyHost = null;
                    Integer SysHttpsProxyPort = null;
                    String SysProxyHost = null;
                    Integer SysProxyPort = null;
                    
                    if (!SysProxyInfoHTTP.equals("DIRECT")) 
                    {
                        String[] SysProxyValuesString = SysProxyInfoHTTP.split(" @ "); //eg.: HTTP @ 10.96.3.105:8080
                        String SysProxyProtokol = SysProxyValuesString[0];
                        String SysProxySetting = SysProxyValuesString[1];
                        String[] SysProxyString = SysProxySetting.split(":"); //eg.: 10.96.3.105:8080
                        SysProxyHost = SysProxyString[0];
                        SysProxyPort = Integer.parseInt(SysProxyString[1]);
                    }
                    
                    if (!SysProxyInfoHTTPS.equals("DIRECT")) 
                    {
                        String[] SysHttpsProxyValuesString = SysProxyInfoHTTPS.split(" @ "); //eg.: HTTP @ 10.96.3.107:7070
                        String SysHttpsProxyProtokol = SysHttpsProxyValuesString[0];
                        String SysHttpsProxySetting = SysHttpsProxyValuesString[1];
                        String[] SysHttpsProxyString = SysHttpsProxySetting.split(":"); //eg.: 10.96.3.105:8080
                        SysHttpsProxyHost = SysHttpsProxyString[0];
                        SysHttpsProxyPort = Integer.parseInt(SysHttpsProxyString[1]);
                    }
                    
                    if (SysHttpsProxyHost != null && SysHttpsProxyPort != -1)
                    {
                    	proxyConfig = new ProxyConfig(SysHttpsProxyHost, SysHttpsProxyPort, "https");
                        monitor.log("OK: Es wird der HTTPS-Proxy vom System benutzt");
                        Logger.info("Hanseatic: Es wird der HTTPS-Proxy vom System benutzt");
                    } 
                    else if (SysProxyHost != null && SysProxyPort != -1)
                    {
                    	proxyConfig = new ProxyConfig(SysProxyHost, SysProxyPort, "http");
                        monitor.log("Warnung: Es wird der HTTP-Proxy vom System benutzt. Sollte dieser kein HTTPS unterst\u00FCzen gibt es Fehler!");
                        Logger.warn("Hanseatic: Es wird der HTTP-Proxy vom System benutzt. Sollte dieser kein HTTPS unterst\u00FCzen gibt es Fehler!");
                    }
                    else 
                    { 
                        throw new Exception("Systemproxy-Einstellungen verwenden ist gew\u00E4hlt: aber bei diesen fehlt offensichtlich ein Eintrag!");
                    }
                }
            } 
            else if (httpsProxyHost != null && httpsProxyPort != -1)
            {
            	proxyConfig = new ProxyConfig(httpsProxyHost, httpsProxyPort, "https");
                monitor.log("OK: Es wird der HTTPS-Proxy von Jameica benutzt");
                Logger.info("Hanseatic: Es wird der HTTPS-Proxy von Jameica benutzt");
            } 
            else if (httpProxyHost != null && httpProxyPort != -1) 
            {
            	proxyConfig = new ProxyConfig(httpProxyHost, httpProxyPort, "http");
                monitor.log("Warnung: Es wird der HTTP-Proxy von Jameica benutzt. Sollte dieser kein HTTPS unterst\u00FCzen gibt es Fehler!");
                Logger.warn("Hanseatic: Es wird der HTTP-Proxy von Jameica benutzt. Sollte dieser kein HTTPS unterst\u00FCzen gibt es Fehler!");
            }
            
            // WebClient mit den den Proxy-Einstellungen anlegen
            if (proxyConfig != null)
            {
            	webClient.getOptions().setProxyConfig(proxyConfig);
            }
                
            Logger.info("Hanseatic: Verbindung vorbereitet");
            monitor.setPercentComplete(2);

            cookieCache = webClient.getCookieManager();
            if (!cookieCache.isCookiesEnabled()) 
            { 
            	cookieCache.setCookiesEnabled(true); 
            }

            String kundenNummer = konto.getKundennummer();

            Wallet wallet = de.willuhn.jameica.hbci.Settings.getWallet();
            Boolean cachePins = de.willuhn.jameica.hbci.Settings.getCachePin();
            Boolean storePins = de.willuhn.jameica.hbci.Settings.getStorePin();
            String walletAlias = "scripting.Hanseatic." + kundenNummer;
            
            monitor.log("Login f\u00fcr " + kundenNummer + " ...");
            
            String passwort = "";
            if (cachePins)
            { 
                passwort = passwortHashtable.get(kundenNummer); 
            } 
            else 
            {
                Logger.debug("Don't cache PINs");
                passwortHashtable.put(kundenNummer,null);
            }
            
            if (storePins) 
            {
                Logger.debug("Store PINs");
                passwort = (String)wallet.get(walletAlias); 
            } 
            else 
            {
                Logger.debug("Don't store PINs");
                if (wallet.get(walletAlias) != null) 
                { 
                	wallet.set(walletAlias,null); 
                }
            }
            
            try 
            {
                if (passwort == null || passwort.equals("")) 
                {
                    Logger.info("Hanseatic: Passwort f\u00fcr Anmeldung "+kundenNummer+" wird abgefragt ...");			
                    
                    passwort = Application.getCallback().askPassword("Bitte geben Sie das Kreditkarten Passwort\n"
                    + "zur Karte " + kundenNummer + "\nein:");
                }
            } 
            catch(Exception err) 
            {
                Logger.error("Hanseatic: Login fehlgeschlagen! Passwort-Eingabe vom Benutzer abgebrochen");
                throw new  java.lang.Exception("Login fehlgeschlagen! Passwort-Eingabe vom Benutzer abgebrochen");
            }

            try 
            {
	            Logger.debug("Hanseatic: besorge Basic-Credentials");
	            HtmlPage pageLogin = webClient.getPage("https://meine.hanseaticbank.de/de/register/sign-in");
	            String textContent = pageLogin.getWebResponse().getContentAsString().replace("\n", "").replace("\r", "");
	            String basicAuth = textContent.replaceAll(".*BASIC_AUTH:\"Basic ([^\"]+)\".*", "$1");
	            String baseUrl = textContent.replaceAll(".*NORTHLAYER_BASE_URL:\"([^\"]+)\".*", "$1");
	            Logger.info("Hanseatic: Basic-Auth-Token ist " + basicAuth + ", BaseUrl: " + baseUrl);
	            
	            WebRequest loginRequest = buildRequest(baseUrl + "/token", HttpMethod.POST, "Basic " + basicAuth, "application/x-www-form-urlencoded; charset=UTF-8", "grant_type=hbSCACustomPassword&password=" + passwort + "&loginId=" + kundenNummer);
	            Page page = webClient.getPage(loginRequest);
	            if (page.getWebResponse().getStatusCode() != 200)
	            {
	            	Logger.error("Hanesatic: Token meldet " + page.getWebResponse().getStatusCode() + " / " + page.getWebResponse().getContentAsString());
	            }
	            textContent = page.getWebResponse().getContentAsString();
	            JSONObject json = new JSONObject(textContent);
	            String token = json.optString("access_token");
	            if (token == null || token.isBlank())
	            {
	            	Logger.error("Hanseatic: Login fehlgeschlagen! " + textContent);
	            	monitor.log("Login fehlgeschlagen, kein Token erhalten!");
	            	throw new  java.lang.Exception("Login fehlgeschlagen! Kein Token erhalten");
	            }
	
	            String tokenType = json.optString("token_type");
	            if (tokenType == null)
	            {
	             	tokenType ="Bearer";
	            }
	            token = tokenType + " " + token;
	
	            if (cachePins) { passwortHashtable.put(kundenNummer, passwort); }
	            if (storePins) { wallet.set(walletAlias, passwort); }
	            
	            monitor.setPercentComplete(5); 
	
	            Logger.info("Hanseatic: Login f\u00fcr " + kundenNummer + " war erfolgreich");
	            monitor.log("Login war erfolgreich");
	
	            WebRequest activateCreditCardsRequest = buildRequest(baseUrl + "/pairingSecureApp/1.0/activateCreditCards", HttpMethod.PUT, token, null, null);
	            page = webClient.getPage(activateCreditCardsRequest);
	            if (page.getWebResponse().getStatusCode() != 200)
	            {
	            	Logger.error("Hanesatic: ActivateCreditCards meldet " + page.getWebResponse().getStatusCode() + " / " + page.getWebResponse().getContentAsString());
	            }
	
	            WebRequest accountsRequest = buildRequest(baseUrl + "/customerportal/1.0/accounts?skipCache=false", HttpMethod.GET, token, null, null);
	            page = webClient.getPage(accountsRequest);
	            if (page.getWebResponse().getStatusCode() != 200)
	            {
	            	Logger.error("Hanesatic: Accounts meldet " + page.getWebResponse().getStatusCode() + " / " + page.getWebResponse().getContentAsString());
	            }
	            Boolean found = false;
	            JSONArray accountsArray = new JSONArray(page.getWebResponse().getContentAsString());
	            Double saldo = konto.getSaldo();
	            for (int i = 0; i < accountsArray.length(); i++) 
	            {
	                JSONObject obj = accountsArray.getJSONObject(i);
	                if (kundenNummer.equals(obj.optString("customerNumber")) && konto.getKontonummer().equals(obj.optString("accountNumber")))
	        		{
                		saldo = obj.optDouble("saldo");
	                	if (fetchSaldo)
	                	{
		                	konto.setSaldo(saldo);
		                	konto.setSaldoAvailable(obj.optDouble("availableAmount"));
		                	found = true;
		                    konto.store();
		                    Application.getMessagingFactory().sendMessage(new SaldoMessage(konto));
	                	}
	                    break;
	        		}
	            };
	
	            if (!found)
	            {
	            	Logger.error("Hanseatic: Konto nicht gefunden, erwartet " + kundenNummer + " / " + konto.getKontonummer() + ", gefunden " + page.getWebResponse().getContentAsString());
	            	monitor.log("Konto nicht gefunden, Saldo kann nicht ermittelt werden");
	            }
	            monitor.setPercentComplete(10);

	            if (fetchUmsatz)
	            {
	            	Double arbeitsSaldo = saldo;
		            Boolean gotDuplicate = false;
		            Boolean more = false;
		            Boolean moreWithSCA = false;
		            Boolean scaDone = false;
		            int pageNo = 0;
		            ArrayList<Umsatz> neueUmsaetze = new ArrayList<Umsatz>();
	            	SimpleDateFormat dateFormat = new SimpleDateFormat("dd.MM.yyyy");
		            do
		            {
		            	pageNo++;
		                
		            	WebRequest umsatzRequest = buildRequest(baseUrl + "/transaction/1.0/transactionsEnriched/" + konto.getKontonummer() + "?page=" + pageNo + "&withReservations=true&withEnrichments=true", HttpMethod.GET, token, null, null);
		            	page = webClient.getPage(umsatzRequest);
		                if (page.getWebResponse().getStatusCode() != 200)
		                {
		                	Logger.error("Hanesatic: Umsätze meldet " + page.getWebResponse().getStatusCode() + " / " + page.getWebResponse().getContentAsString());
		                }
		                json = new JSONObject(page.getWebResponse().getContentAsString());
		                JSONArray transactionsArray = json.optJSONArray("transactions");
		                more = json.optBooleanObject("more");
		                moreWithSCA = json.optBooleanObject("moreWithSCA");
		                
		                Logger.info("Hanseatic: lese Seite "+pageNo);
		                monitor.log("lese Seite" + pageNo);
		                
		                for (int i = 0; i < transactionsArray.length(); i++)
		                {
		                	JSONObject transaction = transactionsArray.getJSONObject(i);
		                	
		                	Double betrag = transaction.getDouble("amount");
		                	Umsatz newUmsatz = (Umsatz) Settings.getDBService().createObject(Umsatz.class,null);
		                    newUmsatz.setKonto(konto);
		                    newUmsatz.setBetrag(betrag);
		                    newUmsatz.setDatum(dateFormat.parse(transaction.optString("transactionDate")));
		                    newUmsatz.setGegenkontoBLZ(transaction.optString("recipientBic"));
		                    newUmsatz.setGegenkontoName(transaction.optString("recipientName"));
		                    newUmsatz.setGegenkontoNummer(transaction.optString("recipientIban"));
		                    newUmsatz.setSaldo(arbeitsSaldo); // Zwischensaldo
		                    newUmsatz.setValuta(dateFormat.parse(transaction.optString("date") + " 00:00"));
		                    newUmsatz.setZweck(transaction.optString("description"));
		                    newUmsatz.setArt(transaction.optString("creditDebitKey"));

		                    ArrayList<String> details = new ArrayList<String>();

		                    String website = transaction.getJSONObject("merchantData").getString("website");
		                    if (website != null && !website.isBlank()) details.add("Web: " + website);
//		                    String location = transaction.optString("location");
//		                    if (location != null && !location.isBlank()) details.add("Ort: " + location);
		                    String transactionTime = transaction.optString("transactionTime");
		                    if (transactionTime != null && !transactionTime.isBlank()) details.add("Zeit: " + transactionTime);
		                    String conversionRate = transaction.optString("conversionRate");
		                    if (conversionRate != null && !conversionRate.isBlank()) 
	                    	{
		                    	details.add("Wechselkurs: " + conversionRate);
//			                    String foreignAmount = transaction.optString("foreignAmount");
//			                    if (foreignAmount != null && !foreignAmount.isBlank()) details.add("Originalbetrag: " + foreignAmount);
	                    	}
		                    
		                    newUmsatz.setCreditorId(transaction.optString("creditorID"));
		                    newUmsatz.setMandateId(transaction.optString("mandateReference"));
		                    newUmsatz.setCustomerRef(transaction.optString("transactionId"));
		                    
		                    for (int j = 0; j < details.size(); j++)
		                    {
		                    	if (details.get(j).length() > 35) 
		                    	{
		                    		details.add(j + 1, details.get(j).substring(35));
		                    		details.set(j, details.get(j).substring(0,35));
		                    	}
		                    }
		                    newUmsatz.setWeitereVerwendungszwecke(details.toArray(new String[0]));
		                    if (transaction.optBoolean("booked") == false)
		                	{
		                    	newUmsatz.setFlags(Umsatz.FLAG_NOTBOOKED);
		                	}
		
		                	arbeitsSaldo -= betrag;
		
		                	if (checkDuplicate(umsaetze, newUmsatz))
		                	{
		                		gotDuplicate = true;
		                	}
		                	else
		                	{
		                		neueUmsaetze.add(newUmsatz);
		                	}
		                }
		                
		                if (!gotDuplicate && !more && moreWithSCA && !scaDone)
		                {
		                    monitor.setPercentComplete(50); 
		                    monitor.log("Benötige zweiten Faktor für weitere Umsätze...");
		                    Logger.info("Hanseatic: Benötige zweiten Faktor für weitere Umsätze...");
		                	
		                    String status = null;
		                    int resultCode = 0;
		                    
		                    do 
		                    {
		                    	WebRequest sessionRequest = buildRequest(baseUrl + "/scaBroker/1.0/session", HttpMethod.POST, token, "application/json", "{\"initiator\":\"ton-sca-fe\",\"lang\":\"de\",\"session\":\"" + token + "\"}");
		                    	page = webClient.getPage(sessionRequest);
		    	                if (page.getWebResponse().getStatusCode() != 200)
		    	                {
		    	                	Logger.error("Hanesatic: Umsätze meldet " + page.getWebResponse().getStatusCode() + " / " + page.getWebResponse().getContentAsString());
		    	                }
		    	                json = new JSONObject(page.getWebResponse().getContentAsString());	    	                
		    	                String type = json.optString("scaType");
		    	                String uniqueId = json.optString("scaUniqueId");
		
		    	                String requestText = "Bitte geben Sie das Einmal-Passwort\n"
			                            + "ein, das sie per " + type + " erhalten haben.";
		    	                if (status != null)
		    	                {
		    	                	requestText += "\nDer letzte Code wurde nicht akzeptiert, Status " + resultCode + " / " + status;
		    	                }
		    	                
		    	                String sca = Application.getCallback().askPassword("Bitte geben Sie das Einmal-Passwort\n"
			                            + "ein, das sie per " + type + " erhalten haben.");
		
			                    if (sca == null || sca.isBlank())
			                    {
			                    	more = false;
			                    	moreWithSCA = false;
			                    	break;
			                    }
			                    else
			                    {
			                    	WebRequest scaRequest = buildRequest(baseUrl + "/scaBroker/1.0/status/" + uniqueId, HttpMethod.PUT, token, "application/json", "{\"otp\":\"" + sca + "\"}");
			                    	page = webClient.getPage(scaRequest);
			                    	if (page.getWebResponse().getStatusCode() != 200)
			    	                {
			    	                	Logger.error("Hanesatic: Umsätze meldet " + page.getWebResponse().getStatusCode() + " / " + page.getWebResponse().getContentAsString());
			    	                }
			    	                json = new JSONObject(page.getWebResponse().getContentAsString());
			    	                status = json.optString("status");
			    	                resultCode = json.optInt("resultCode");
			    	                
			    	                if (!"complete".equals(status) || resultCode != 200)
			    	                {
			    	                	Logger.error("Hanseatic: ungültige Antwort auf SCA-Request: " + page.getWebResponse().getContentAsString());
			    	                	monitor.log("ungültige Antwort auf SCA-Request");
			    	                }
			    	                else
			    	                {
			    	                	scaDone = true;
			    	                	neueUmsaetze.clear();
				                		arbeitsSaldo = saldo;
			    	                	pageNo = 0;
			    	                	monitor.log("Lese Daten erneut nach Eingabe zweiter Faktor");
			    	                	Logger.info("Hanseatic: Lese Daten erneut nach Eingabe zweiter Faktor");
			    	                	break;
			    	                }
			                    }
		                    }
		                    while (true);
		                }	                
		            } while (!gotDuplicate && (more || moreWithSCA));
		            
		            monitor.setPercentComplete(75); 
		            Logger.info("Hanseatic: Kontoauszug erfolgreich. Importiere Daten ...");
		            monitor.log("Kontoauszug erfolgreich. Importiere Daten ...");
		
		            for (int i = neueUmsaetze.size() - 1; i >= 0; i--)
		            {
		            	Umsatz umsatz = neueUmsaetze.get(i); 
			    		umsatz.store();
			    		Application.getMessagingFactory().sendMessage(new ImportMessage(umsatz));
		            }
	            }
	            
	            monitor.log("******************************************************************************************************************\n\n\n");
	            monitor.addPercentComplete(100); 
            } 
            catch (ParseException | RemoteException | ApplicationException ex) 
            {
            	Logger.error("Hanseatic: Fehler beim Abrufen der Umsätze: " + ex.toString());
            }
        }

        private WebRequest buildRequest(String url, HttpMethod method, String auth, String contentType, String data) throws MalformedURLException, URISyntaxException
        {
            WebRequest request = new WebRequest(new java.net.URI(url).toURL(), method);
            request.setAdditionalHeaders(new Hashtable<String, String>());
            request.setAdditionalHeader("Accept", "application/json");
            request.setAdditionalHeader("Accept-Language", "de-DE,de;q=0.9,en-US;q=0.8,en;q=0.7");
            if (auth != null)
            {
            	request.setAdditionalHeader("authorization", auth);
            }
            if (contentType != null && data != null)
            {
            	request.setAdditionalHeader("content-type", contentType);
            	request.setRequestBody(data);
            }
            return request;
		}

		protected Boolean checkDuplicate(DBIterator<Umsatz> umsaetze, Umsatz buchung) throws RemoteException
        {
            umsaetze.begin();
            while (umsaetze.hasNext())
            {
                Umsatz buchung2 = umsaetze.next();
                
                long d1 = buchung2.getDatum().getTime();
                long d2 = buchung.getDatum().getTime();
                String z1 = buchung2.getZweck();
                String z2 = buchung.getZweck();
                Double b1 = buchung2.getBetrag();
                Double b2 = buchung.getBetrag();
                int f1 = buchung2.getFlags() & Umsatz.FLAG_NOTBOOKED;
                int f2 = buchung.getFlags() & Umsatz.FLAG_NOTBOOKED;
                if (b1.equals(b2) &&
                    d1 == d2 &&
                    z1.equals(z2) &&
                    f1 == f2
                )
                {
                    return true;
                }
            }
            return false;
        }
    }
}
