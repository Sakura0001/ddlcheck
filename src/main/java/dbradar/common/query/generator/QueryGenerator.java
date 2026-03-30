package dbradar.common.query.generator;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Stack;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.luaj.vm2.Globals;
import org.luaj.vm2.LuaValue;
import org.luaj.vm2.Varargs;
import org.luaj.vm2.lib.VarArgFunction;
import org.luaj.vm2.lib.jse.JsePlatform;

import dbradar.GlobalState;
import dbradar.IgnoreMeException;
import dbradar.Randomly;
import grammar.Grammar;
import grammar.Production;
import grammar.Sequence;
import grammar.Token;
import grammar.Token.TokenType;
import dbradar.common.query.generator.data.Generator;
import dbradar.common.query.generator.data.GeneratorRegister;
import dbradar.common.schema.AbstractTableColumn;

public class QueryGenerator {

    private GlobalState globalState;
    private Grammar grammar;
    private String queryRoot;
    private int maxRecursion;
    private Map<Integer, Integer> seqCounter;

    private AbstractKeyFuncManager funcManager;
    private Globals luaGlobal;
    private StringBuilder luaPrintBuffer;

    public QueryGenerator(GlobalState globalState, Grammar grammar, String queryRoot, int maxRecursion,
                          AbstractKeyFuncManager funcManager) {
        this.globalState = globalState;
        this.grammar = grammar;
        this.queryRoot = queryRoot;
        this.maxRecursion = maxRecursion;
        this.funcManager = funcManager;
        this.seqCounter = globalState.getSeqCounter(queryRoot.hashCode());
        initializeLua();
    }

    public QueryGenerator(GlobalState globalState, Grammar grammar, String queryRoot,
                          AbstractKeyFuncManager funcManager) {
        this(globalState, grammar, queryRoot, 3, funcManager);
    }

    private void initializeLua() {
        luaGlobal = JsePlatform.standardGlobals();

        luaPrintBuffer = new StringBuilder();
        MyLuaPrint myPrint = new MyLuaPrint(luaPrintBuffer);
        luaGlobal.set("print", myPrint);
        for (Token codeBlock : grammar.getHeadCodeBlocks()) {
            String luaStr = codeBlock.getValue().substring(1, codeBlock.getValue().length() - 1); // remove {}
            luaGlobal.load(luaStr).call();
        }

        // register data generators
        Map<String, Generator> generators = GeneratorRegister.getBasicGenerators(globalState);
        for (String generatorName : generators.keySet()) {
            MyFunction myFunc = new MyFunction();
            myFunc.generator = generators.get(generatorName);
            luaGlobal.set(generatorName, myFunc);
        }

        // register _column key func
        MyFunction column = new MyFunction();
        column.generator = (GlobalState state) -> {
            List<AbstractTableColumn<?, ?>> columns = funcManager.getCurrentContext().getCurrentColumns();
            AbstractTableColumn<?, ?> col = Randomly.fromList(columns);
            return col.getName();
        };

        luaGlobal.set("column", column);
    }

    public ASTNode getRandomQuery() {
        Production prod = grammar.getProductionBySymbol(queryRoot);
        if (prod == null) {
            throw new RuntimeException(String.format("QueryGenerator: production %s not found", queryRoot));
        }

        ASTNode rootNode = new ASTNode(prod.getSymbol());

        Stack<Sequence> chosenSequences = new Stack<>();

        try {
            internalGenerate(rootNode, rootNode, chosenSequences);

            // Fill all fillers in the default context.
            funcManager.cleanupFillers();
            return rootNode;
        } catch (QueryGenerationException | IgnoreMeException e) {
            throw e;
        }
    }

    private void internalGenerate(ASTNode rootNode, ASTNode curNode, Stack<Sequence> chosenSequences) {
        Production prod = grammar.getProductionBySymbol(curNode.getToken().getValue());

        List<Sequence> availableSequences = new ArrayList<>();
        for (Sequence seq : prod.getSequences()) {
            if (canUseSequence(curNode, seq)) {
                availableSequences.add(seq);
            }
        }
        if (availableSequences.isEmpty()) {
            throw new RuntimeException("QueryGenerator: There must be available sequences. Something is wrong!");
        }

        Sequence chosenSequence;
        if (seqCounter != null) {
            Map<Sequence, Integer> weightedSequences = new HashMap<>();
            for (Sequence seq : availableSequences) {
                if (!seqCounter.containsKey(seq.hashCode())) {
                    seqCounter.put(seq.hashCode(), 0);
                }
                weightedSequences.put(seq, seqCounter.get(seq.hashCode()));
            }

            List<Sequence> candidates = new ArrayList<>();
            for (Sequence seq : weightedSequences.keySet()) {
                if (weightedSequences.get(seq) == 0) {
                    candidates.add(seq);
                }
            }

            if (!candidates.isEmpty()) {
                chosenSequence = Randomly.fromList(candidates);
            } else {
                chosenSequence = Randomly.fromList(availableSequences);
            }

            seqCounter.put(chosenSequence.hashCode(), seqCounter.get(chosenSequence.hashCode()) + 1);
        } else {
            chosenSequence = Randomly.fromList(availableSequences);
        }

        chosenSequences.push(chosenSequence);

        List<Token> tokens = new ArrayList<>();
        for (Token curToken : chosenSequence.getTokens()) {
            if (curToken.getMultiplicity() == Token.MultiplicityType.ASTERISK) {
                int time = Randomly.getNotCachedInteger(0, 3);
                while (time-- > 0) {
                    tokens.add(curToken);
                }
            } else if (curToken.getMultiplicity() == Token.MultiplicityType.PLUS) {
                Token firstToken = curToken.clone();
                firstToken.setMultiplicity(Token.MultiplicityType.NORMAL);
                tokens.add(firstToken); // Must have one token

                int time = Randomly.getNotCachedInteger(0, 1);
                while (time-- > 0) {
                    tokens.add(curToken);
                }
            } else if (curToken.getMultiplicity() == Token.MultiplicityType.OPTIONAL) {
                if (Randomly.getBoolean()) {
                    tokens.add(curToken);
                }
            } else {
                tokens.add(curToken);
            }
        }

        try {
            for (Token token : tokens) {
                if (token.hasType(TokenType.TERMINAL)) {
                    ASTNode node = new ASTNode(token);
                    curNode.addChild(node);
                    try {
                        validateQuery(rootNode, chosenSequences);
                    } catch (QueryGenerationException e) {
                        curNode.removeChild(node);
                        if (token.getMultiplicity() == Token.MultiplicityType.NORMAL) {
                            throw e;
                        }
                    }
                } else if (token.hasType(TokenType.KEYWORD)) {
                    ASTNode node = new ASTNode(token);
                    curNode.addChild(node);
                    KeyFunc func = funcManager.getFuncByKey(token.getValue());

                    try {
                        func.generateAST(node);
                        validateQuery(rootNode, chosenSequences);
                    } catch (QueryGenerationException e) {
                        // If a token is mandatory, the AST generation fails. If a token is
                        // optional, we ignore this token.
                        curNode.removeChild(node);
                        if (token.getMultiplicity() == Token.MultiplicityType.NORMAL) {
                            throw e;
                        }
                    }
                } else if (token.hasType(TokenType.CODE_BLOCK)) {
                    // lua code block
                    String luaStr = token.getValue().substring(1, token.getValue().length() - 1);
                    luaGlobal.load(luaStr).call();

                    if (!luaPrintBuffer.toString().isEmpty()) {
                        ASTNode node = new ASTNode(token);
                        curNode.addChild(node);

                        Token childToken = new Token(TokenType.TERMINAL, luaPrintBuffer.toString());
                        ASTNode childNode = new ASTNode(childToken);
                        node.addChild(childNode);

                        luaPrintBuffer.setLength(0);
                        try {
                            validateQuery(rootNode, chosenSequences);
                        } catch (QueryGenerationException e) {
                            curNode.removeChild(node);
                            if (token.getMultiplicity() == Token.MultiplicityType.NORMAL) {
                                throw e;
                            }
                        }
                    }
                } else {
                    ASTNode node = new ASTNode(token);
                    curNode.addChild(node);
                    try {
                        internalGenerate(rootNode, node, chosenSequences);
                    } catch (QueryGenerationException e) {
                        // If a token is mandatory, the AST generation fails. If a token is
                        // optional, we ignore this token.
                        curNode.removeChild(node);
                        if (token.getMultiplicity() == Token.MultiplicityType.NORMAL) {
                            throw e;
                        }
                    }
                }
            }
        } finally {
            chosenSequences.pop();
        }
    }

    private boolean canUseSequence(ASTNode parentNode, Sequence seq) {
        if (!isAllowed(parentNode, seq)) {
            return false;
        }

        return canTerminateInMaxRecursion(parentNode, seq);
    }

    private boolean isAllowed(ASTNode parentNode, Sequence seq) {
        if (globalState.getCurrentOracle() != null) {
            String oracleName = globalState.getCurrentOracle().toLowerCase();
            if (seq.getDisabledOracles().contains(oracleName)) {
                return false;
            }
            if (!seq.getEnabledOracles().isEmpty() && !seq.getEnabledOracles().contains(oracleName)) {
                return false;
            }
        }

        ASTNode pNode = new ASTNode(parentNode);
        while (pNode != null) {
            String symbol = pNode.getToken().getValue();
            if (seq.getDisabledSymbols().contains(symbol)) {
                return false;
            }
            pNode = pNode.getParent();
        }

        return true;
    }

    private boolean canTerminateInMaxRecursion(ASTNode parentNode, Sequence seq) {
        boolean canTerminate = true;
        ASTNode pNode = new ASTNode(parentNode); // Clone the parent node, to avoid affecting the query generation.
        for (Token token : seq.getTokens()) {
            if (token.hasType(TokenType.NON_TERMINAL)) {
                ASTNode childNode = new ASTNode(token);
                pNode.addChild(childNode);
                if (exceedMaxRecursion(childNode)) {
                    canTerminate = false;
                    break;
                } else {
                    Production prod = grammar.getProductionBySymbol(token.getValue());
                    boolean hasAvailableSeq = false;
                    for (Sequence tmpSeq : prod.getSequences()) {
                        if (isAllowed(childNode, tmpSeq)) {
                            if (canTerminateInMaxRecursion(childNode, tmpSeq)) {
                                hasAvailableSeq = true;
                                break;
                            }
                        }
                    }
                    if (!hasAvailableSeq) {
                        canTerminate = false;
                        break;
                    }
                }
            }
        }

        return canTerminate;
    }

    private boolean exceedMaxRecursion(ASTNode node) {
        Map<String, Integer> prodCount = new HashMap<>();
        while (node != null && node.getToken().hasType(TokenType.NON_TERMINAL)) {
            String symbol = node.getToken().getValue();
            Integer count = prodCount.get(symbol);
            count = count == null ? 1 : count + 1;
            prodCount.put(symbol, count);
            if (count > maxRecursion) {
                return true;
            }
            node = node.getParent();
        }
        return false;
    }

    private void validateQuery(ASTNode rootNode, Stack<Sequence> sequences) {
        List<String> queryPatterns = new ArrayList<>();
        for (Sequence seq : sequences) {
            queryPatterns.addAll(seq.getDisabledQueryPatterns());
        }

        String query = rootNode.toQueryString();
        for (String queryPattern : queryPatterns) {
            Pattern pattern = Pattern.compile(queryPattern);
            Matcher matcher = pattern.matcher(query);
            if (matcher.find()) {
                throw new QueryGenerationException(
                        "The generated query satisfies a violation pattern: " + queryPattern);
            }
        }
    }

    private class MyFunction extends VarArgFunction {
        private Generator generator;

        @Override
        public Varargs invoke(Varargs args) {
            return LuaValue.valueOf(generator.generate(globalState));
        }
    }

    private class MyLuaPrint extends VarArgFunction {

        private StringBuilder sb;

        MyLuaPrint(StringBuilder sb) {
            this.sb = sb;
        }

        @Override
        public Varargs invoke(Varargs args) {
            String value = args.tojstring(1);
            sb.append(value);
            return NIL;
        }
    }

}
