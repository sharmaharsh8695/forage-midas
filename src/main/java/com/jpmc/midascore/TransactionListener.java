package com.jpmc.midascore;

import com.jpmc.midascore.entity.TransactionRecord;
import com.jpmc.midascore.entity.UserRecord;
import com.jpmc.midascore.foundation.Incentive;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.kafka.annotation.KafkaListener;
import com.jpmc.midascore.foundation.Transaction;
import org.springframework.transaction.annotation.Transactional;
import com.jpmc.midascore.repository.TransactionRepository;
import com.jpmc.midascore.repository.UserRepository;
import com.jpmc.midascore.config.RestConfig;
import org.springframework.web.client.RestTemplate;

import java.util.Optional;



@Component
public class TransactionListener {

    private final UserRepository userRepository;
    private final TransactionRepository transactionRepository;
    private final RestTemplate restTemplate;

    public TransactionListener(
            UserRepository userRepository,
            TransactionRepository transactionRepository,
            RestTemplate restTemplate
    ) {
        this.userRepository = userRepository;
        this.transactionRepository = transactionRepository;
        this.restTemplate = restTemplate;
    }

    @KafkaListener(
            topics = "${general.kafka-topic}",
            groupId = "midas-core"
    )
    @Transactional
    public void listen(Transaction transaction) {
        Optional<UserRecord> senderOpt = userRepository.findById(transaction.getSenderId());
        Optional<UserRecord> recipientOpt = userRepository.findById(transaction.getRecipientId());

        if (senderOpt.isEmpty() || recipientOpt.isEmpty()) {
            return;
        }


        UserRecord sender = senderOpt.get();
        UserRecord recipient = recipientOpt.get();

        float amount = transaction.getAmount();

        if (sender.getBalance() < amount) {
            return;
        }

        Incentive incentive = restTemplate.postForObject(
                "http://localhost:8080/incentive",
                transaction,
                Incentive.class
        );
        float incentiveAmount = incentive.getAmount();


        sender.setBalance(sender.getBalance() - amount);
        recipient.setBalance(recipient.getBalance() + amount + incentiveAmount);

        TransactionRecord record =
                new TransactionRecord(sender, recipient, amount , incentiveAmount);
        transactionRepository.save(record);


//        System.out.println("........................................................................."+ userRepository.findById(9).get().getBalance() + ".......................................................................");

    }
}
